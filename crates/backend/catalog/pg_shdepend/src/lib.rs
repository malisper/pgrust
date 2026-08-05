// pg_shdepend.c recording/mutation/report slice plus shdepDropOwned and
// shdepReassignOwned; the getObjectDescription arms cover only
// relation/schema/database/role.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::{
    AttrNumber, InvalidOid, Oid, OidIsValid, AUTH_ID_RELATION_ID, DATABASE_RELATION_ID,
    NAMESPACE_RELATION_ID, RELATION_RELATION_ID, TABLE_SPACE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_UNDEFINED_OBJECT,
};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, Relation, RowExclusiveLock, LOCKMODE,
};
use types_nodes::parsenodes::DropBehavior;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, ItemPointerData, TupleDescData};

// dependency.c / aclchk.c callees used by shdepDropOwned; decls live here
// because dependency_seams/aclchk_seams sit below crates that depend on this
// one. Installed by catalog_dependency::init_seams and aclchk::init_seams.
seam_core::seam!(
    pub fn acquire_deletion_lock(class_id: Oid, object_id: Oid, flags: i32) -> PgResult<()>
);
seam_core::seam!(
    pub fn release_deletion_lock(class_id: Oid, object_id: Oid) -> PgResult<()>
);
seam_core::seam!(
    pub fn perform_multiple_deletions(
        mcx: Mcx<'_>,
        objects: &[(Oid, Oid, i32)],
        behavior: DropBehavior,
        flags: i32,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn remove_role_from_object_acl(
        mcx: Mcx<'_>,
        roleid: Oid,
        classid: Oid,
        objid: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn remove_role_from_init_priv(
        mcx: Mcx<'_>,
        roleid: Oid,
        classid: Oid,
        objid: Oid,
        objsubid: i32,
    ) -> PgResult<()>
);

// shdepReassignOwned_Owner callees; each owning command crate installs its
// own in init_seams (they all sit above this crate via changeDependencyOnOwner).
seam_core::seam!(
    pub fn alter_type_owner_oid(
        mcx: Mcx<'_>,
        type_oid: Oid,
        new_owner_id: Oid,
        has_depend_entry: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_schema_owner_oid(mcx: Mcx<'_>, schemaoid: Oid, new_owner_id: Oid) -> PgResult<()>
);
seam_core::seam!(
    pub fn at_exec_change_owner(
        mcx: Mcx<'_>,
        relation_oid: Oid,
        new_owner_id: Oid,
        recursing: bool,
        lockmode: LOCKMODE,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_foreign_server_owner_oid(
        mcx: Mcx<'_>,
        srv_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_foreign_data_wrapper_owner_oid(
        mcx: Mcx<'_>,
        fdw_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_event_trigger_owner_oid(
        mcx: Mcx<'_>,
        trig_oid: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_publication_owner_oid(
        mcx: Mcx<'_>,
        pub_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_subscription_owner_oid(
        mcx: Mcx<'_>,
        sub_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn alter_object_owner_internal(
        mcx: Mcx<'_>,
        class_id: Oid,
        object_id: Oid,
        new_owner_id: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    pub fn replace_role_in_init_priv(
        mcx: Mcx<'_>,
        oldroleid: Oid,
        newroleid: Oid,
        classid: Oid,
        objid: Oid,
        objsubid: i32,
    ) -> PgResult<()>
);
// xact sits above this crate; installed by catalog_dependency::init_seams.
seam_core::seam!(pub fn command_counter_increment() -> PgResult<()>);

#[cfg(test)]
mod tests;

const Natts_pg_shdepend: usize = 7;
const Anum_pg_shdepend_dbid: usize = 1;
const Anum_pg_shdepend_classid: usize = 2;
const Anum_pg_shdepend_objid: usize = 3;
const Anum_pg_shdepend_objsubid: usize = 4;
const Anum_pg_shdepend_refclassid: usize = 5;
const Anum_pg_shdepend_refobjid: usize = 6;
const Anum_pg_shdepend_deptype: usize = 7;

const DEFAULTTABLESPACE_OID: Oid = 1663;
const MAX_REPORTED_DEPS: i32 = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SharedDependencyType {
    Owner,
    Acl,
    InitAcl,
    Policy,
    Tablespace,
}

impl SharedDependencyType {
    pub const fn as_char(self) -> i8 {
        (match self {
            SharedDependencyType::Owner => b'o',
            SharedDependencyType::Acl => b'a',
            SharedDependencyType::InitAcl => b'i',
            SharedDependencyType::Policy => b'r',
            SharedDependencyType::Tablespace => b't',
        }) as i8
    }
}

// C's ShDependObjectInfo objtype (LOCAL/SHARED/REMOTE) is dropped: local and
// shared entries render identically and remote ones ride a separate arm.
#[derive(Clone, Copy)]
struct ShDependObjectInfo {
    classId: Oid,
    objectId: Oid,
    objectSubId: i32,
    deptype: i8,
}

struct RemoteDep {
    dbOid: Oid,
    count: i32,
}

struct FormPgShdepend {
    dbid: Oid,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
    refclassid: Oid,
    refobjid: Oid,
    deptype: i8,
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

fn oid_key(attno: usize, oid: Oid) -> ScanKeyData {
    scankey(attno, types_core::fmgr::F_OIDEQ, Datum::from_oid(oid))
}

fn int4_key(attno: usize, v: i32) -> ScanKeyData {
    scankey(attno, types_core::fmgr::F_INT4EQ, Datum::from_i32(v))
}

fn getattr(tup: &HeapTupleData<'_>, attnum: usize, desc: &TupleDescData<'_>) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_shdepend column under the relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

fn form_pg_shdepend(tup: &HeapTupleData<'_>, desc: &TupleDescData<'_>) -> FormPgShdepend {
    FormPgShdepend {
        dbid: getattr(tup, Anum_pg_shdepend_dbid, desc).as_oid(),
        classid: getattr(tup, Anum_pg_shdepend_classid, desc).as_oid(),
        objid: getattr(tup, Anum_pg_shdepend_objid, desc).as_oid(),
        objsubid: getattr(tup, Anum_pg_shdepend_objsubid, desc).as_i32(),
        refclassid: getattr(tup, Anum_pg_shdepend_refclassid, desc).as_oid(),
        refobjid: getattr(tup, Anum_pg_shdepend_refobjid, desc).as_oid(),
        deptype: getattr(tup, Anum_pg_shdepend_deptype, desc).as_i8(),
    }
}

// recordSharedDependencyOn (pg_shdepend.c): the C ObjectAddress pair is
// flattened to oids since pg_shdepend rows can't carry SubIds.
pub fn recordSharedDependencyOn<'mcx>(
    mcx: Mcx<'mcx>,
    dependerClassId: Oid,
    dependerObjectId: Oid,
    refClassId: Oid,
    refObjId: Oid,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;

    if !catalog::IsPinnedObject(refClassId, refObjId) {
        shdepAddDependency(
            mcx,
            &sdepRel,
            dependerClassId,
            dependerObjectId,
            0,
            refClassId,
            refObjId,
            deptype,
        )?;
    }

    sdepRel.close(RowExclusiveLock)
}

pub fn recordDependencyOnOwner<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    owner: Oid,
) -> PgResult<()> {
    recordSharedDependencyOn(
        mcx,
        classId,
        objectId,
        AUTH_ID_RELATION_ID,
        owner,
        SharedDependencyType::Owner,
    )
}

fn shdepChangeDep<'mcx>(
    mcx: Mcx<'mcx>,
    sdepRel: &Relation<'mcx>,
    classid: Oid,
    objid: Oid,
    objsubid: i32,
    refclassid: Oid,
    refobjid: Oid,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    let dbid = classIdGetDbId(classid);

    shdepLockAndCheckObject(mcx, refclassid, refobjid)?;

    let keys = [
        oid_key(Anum_pg_shdepend_dbid, dbid),
        oid_key(Anum_pg_shdepend_classid, classid),
        oid_key(Anum_pg_shdepend_objid, objid),
        int4_key(Anum_pg_shdepend_objsubid, objsubid),
    ];

    let desc = sdepRel.descr();
    let mut oldtup: Option<(ItemPointerData, FormPgShdepend)> = None;

    let mut scan = genam::systable_beginscan(
        mcx,
        sdepRel,
        catalog::SharedDependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let form = form_pg_shdepend(tup, desc);
        if form.deptype != deptype.as_char() {
            continue;
        }
        if oldtup.is_some() {
            return Err(Box::new(PgError::error(format!(
                "multiple pg_shdepend entries for object {classid}/{objid}/{objsubid} deptype {}",
                deptype.as_char() as u8 as char
            ))));
        }
        oldtup = Some((tup.t_self, form));
    }
    genam::systable_endscan(mcx, scan)?;

    if catalog::IsPinnedObject(refclassid, refobjid) {
        if let Some((tid, _)) = oldtup {
            catalog_indexing::CatalogTupleDelete(sdepRel, &tid)?;
        }
    } else if let Some((tid, form)) = oldtup {
        // C modifies a heap_copytuple copy in place; an identical row image is
        // rebuilt here instead.
        let values = [
            Datum::from_oid(form.dbid),
            Datum::from_oid(form.classid),
            Datum::from_oid(form.objid),
            Datum::from_i32(form.objsubid),
            Datum::from_oid(refclassid),
            Datum::from_oid(refobjid),
            Datum::from_char(form.deptype),
        ];
        let nulls = [false; Natts_pg_shdepend];
        let mut tup = heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)?;
        catalog_indexing::CatalogTupleUpdate(mcx, sdepRel, &tid, &mut tup)?;
    } else {
        let values = [
            Datum::from_oid(dbid),
            Datum::from_oid(classid),
            Datum::from_oid(objid),
            Datum::from_i32(objsubid),
            Datum::from_oid(refclassid),
            Datum::from_oid(refobjid),
            Datum::from_char(deptype.as_char()),
        ];
        let nulls = [false; Natts_pg_shdepend];
        let mut tup = heaptuple::heap_form_tuple(mcx, desc, &values, &nulls)?;
        catalog_indexing::CatalogTupleInsert(mcx, sdepRel, &mut tup)?;
    }

    Ok(())
}

pub fn changeDependencyOnOwner<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    newOwnerId: Oid,
) -> PgResult<()> {
    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;

    shdepChangeDep(
        mcx,
        &sdepRel,
        classId,
        objectId,
        0,
        AUTH_ID_RELATION_ID,
        newOwnerId,
        SharedDependencyType::Owner,
    )?;

    // A SHARED_DEPENDENCY_ACL entry must never exist for the owner (aclchk
    // skips it); drop any left over from a grant to the new owner.
    shdepDropDependency(
        mcx,
        &sdepRel,
        classId,
        objectId,
        0,
        true,
        AUTH_ID_RELATION_ID,
        newOwnerId,
        Some(SharedDependencyType::Acl),
    )?;

    sdepRel.close(RowExclusiveLock)
}

pub fn recordDependencyOnTablespace<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    tablespace: Oid,
) -> PgResult<()> {
    recordSharedDependencyOn(
        mcx,
        classId,
        objectId,
        TABLE_SPACE_RELATION_ID,
        tablespace,
        SharedDependencyType::Tablespace,
    )
}

pub fn changeDependencyOnTablespace<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    newTablespaceId: Oid,
) -> PgResult<()> {
    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;

    if newTablespaceId != DEFAULTTABLESPACE_OID && newTablespaceId != InvalidOid {
        shdepChangeDep(
            mcx,
            &sdepRel,
            classId,
            objectId,
            0,
            TABLE_SPACE_RELATION_ID,
            newTablespaceId,
            SharedDependencyType::Tablespace,
        )?;
    } else {
        shdepDropDependency(
            mcx, &sdepRel, classId, objectId, 0, true, InvalidOid, InvalidOid, None,
        )?;
    }

    sdepRel.close(RowExclusiveLock)
}

fn getOidListDiff(list1: &mut [Oid], nlist1: &mut usize, list2: &mut [Oid], nlist2: &mut usize) {
    let mut in1 = 0;
    let mut in2 = 0;
    let mut out1 = 0;
    let mut out2 = 0;

    while in1 < *nlist1 && in2 < *nlist2 {
        if list1[in1] == list2[in2] {
            in1 += 1;
            in2 += 1;
        } else if list1[in1] < list2[in2] {
            list1[out1] = list1[in1];
            out1 += 1;
            in1 += 1;
        } else {
            list2[out2] = list2[in2];
            out2 += 1;
            in2 += 1;
        }
    }

    while in1 < *nlist1 {
        list1[out1] = list1[in1];
        out1 += 1;
        in1 += 1;
    }

    while in2 < *nlist2 {
        list2[out2] = list2[in2];
        out2 += 1;
        in2 += 1;
    }

    *nlist1 = out1;
    *nlist2 = out2;
}

// Inputs must be sorted and de-duped (aclmembers guarantees both); borrowed
// slices where C consumes and pfrees.
pub fn updateAclDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    ownerId: Oid,
    oldmembers: &[Oid],
    newmembers: &[Oid],
) -> PgResult<()> {
    updateAclDependenciesWorker(
        mcx,
        classId,
        objectId,
        objsubId,
        ownerId,
        SharedDependencyType::Acl,
        oldmembers,
        newmembers,
    )
}

pub fn updateInitAclDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    oldmembers: &[Oid],
    newmembers: &[Oid],
) -> PgResult<()> {
    updateAclDependenciesWorker(
        mcx,
        classId,
        objectId,
        objsubId,
        InvalidOid,
        SharedDependencyType::InitAcl,
        oldmembers,
        newmembers,
    )
}

fn updateAclDependenciesWorker<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    ownerId: Oid,
    deptype: SharedDependencyType,
    oldmembers: &[Oid],
    newmembers: &[Oid],
) -> PgResult<()> {
    let mut old: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    for &roleid in oldmembers {
        old.push(roleid);
    }
    let mut new: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    for &roleid in newmembers {
        new.push(roleid);
    }
    let mut noldmembers = old.len();
    let mut nnewmembers = new.len();
    getOidListDiff(&mut old, &mut noldmembers, &mut new, &mut nnewmembers);

    if noldmembers > 0 || nnewmembers > 0 {
        let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;

        for &roleid in new.iter().take(nnewmembers) {
            // The owner has an OWNER shdep entry instead of an ACL one (the
            // invariant changeDependencyOnOwner relies on); INITACL records
            // the owner too.
            if deptype == SharedDependencyType::Acl && roleid == ownerId {
                continue;
            }
            if catalog::IsPinnedObject(AUTH_ID_RELATION_ID, roleid) {
                continue;
            }
            shdepAddDependency(
                mcx,
                &sdepRel,
                classId,
                objectId,
                objsubId,
                AUTH_ID_RELATION_ID,
                roleid,
                deptype,
            )?;
        }

        for &roleid in old.iter().take(noldmembers) {
            if deptype == SharedDependencyType::Acl && roleid == ownerId {
                continue;
            }
            if catalog::IsPinnedObject(AUTH_ID_RELATION_ID, roleid) {
                continue;
            }
            shdepDropDependency(
                mcx,
                &sdepRel,
                classId,
                objectId,
                objsubId,
                false,
                AUTH_ID_RELATION_ID,
                roleid,
                Some(deptype),
            )?;
        }

        sdepRel.close(RowExclusiveLock)?;
    }

    Ok(())
}

fn shared_dependency_comparator(
    obja: &ShDependObjectInfo,
    objb: &ShDependObjectInfo,
) -> core::cmp::Ordering {
    obja.objectId
        .cmp(&objb.objectId)
        .then(obja.classId.cmp(&objb.classId))
        .then((obja.objectSubId as u32).cmp(&(objb.objectSubId as u32)))
        .then(obja.deptype.cmp(&objb.deptype))
}

// checkSharedDependencies (pg_shdepend.c): Some((detail, detail_log)) is the
// C true + the two out-params; None is the C false/NULL/NULL.
pub fn checkSharedDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
) -> PgResult<Option<(PgString<'mcx>, PgString<'mcx>)>> {
    if catalog::IsPinnedObject(classId, objectId) {
        return Err(Box::new(
            PgError::error(format!(
                "cannot drop {} because it is required by the database system",
                getObjectDescription(mcx, classId, objectId, 0)?
            ))
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
        ));
    }

    let mut objects: PgVec<'mcx, ShDependObjectInfo> = PgVec::new_in(mcx);
    let mut remDeps: PgVec<'mcx, RemoteDep> = PgVec::new_in(mcx);

    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, AccessShareLock)?;
    let desc = sdepRel.descr();

    let keys = [
        oid_key(Anum_pg_shdepend_refclassid, classId),
        oid_key(Anum_pg_shdepend_refobjid, objectId),
    ];

    let myDatabaseId = init_small::globals::MyDatabaseId();

    let mut scan = genam::systable_beginscan(
        mcx,
        &sdepRel,
        catalog::SharedDependReferenceIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let form = form_pg_shdepend(tup, desc);

        if form.dbid == myDatabaseId || form.dbid == InvalidOid {
            objects.push(ShDependObjectInfo {
                classId: form.classid,
                objectId: form.objid,
                objectSubId: form.objsubid,
                deptype: form.deptype,
            });
        } else if let Some(dep) = remDeps.iter_mut().find(|dep| dep.dbOid == form.dbid) {
            dep.count += 1;
        } else {
            remDeps.push(RemoteDep { dbOid: form.dbid, count: 1 });
        }
    }
    genam::systable_endscan(mcx, scan)?;

    sdepRel.close(AccessShareLock)?;

    if objects.len() > 1 {
        objects.sort_by(shared_dependency_comparator);
    }

    let mut numReportedDeps: i32 = 0;
    let mut numNotReportedDeps: i32 = 0;
    let mut numNotReportedDbs: i32 = 0;
    let mut descs = PgString::new_in(mcx);
    let mut alldescs = PgString::new_in(mcx);

    for obj in objects.iter() {
        if numReportedDeps < MAX_REPORTED_DEPS {
            numReportedDeps += 1;
            storeObjectDescription(mcx, &mut descs, obj)?;
        } else {
            numNotReportedDeps += 1;
        }
        storeObjectDescription(mcx, &mut alldescs, obj)?;
    }

    for dep in remDeps.iter() {
        if numReportedDeps < MAX_REPORTED_DEPS {
            numReportedDeps += 1;
            storeRemoteObjectDescription(mcx, &mut descs, dep.dbOid, dep.count)?;
        } else {
            numNotReportedDbs += 1;
        }
        storeRemoteObjectDescription(mcx, &mut alldescs, dep.dbOid, dep.count)?;
    }

    if descs.is_empty() {
        return Ok(None);
    }

    if numNotReportedDeps > 0 {
        let plural = if numNotReportedDeps == 1 { "object" } else { "objects" };
        descs.try_push_str(&format!(
            "\nand {numNotReportedDeps} other {plural} (see server log for list)"
        ))?;
    }
    if numNotReportedDbs > 0 {
        let plural = if numNotReportedDbs == 1 { "database" } else { "databases" };
        descs.try_push_str(&format!(
            "\nand objects in {numNotReportedDbs} other {plural} (see server log for list)"
        ))?;
    }

    Ok(Some((descs, alldescs)))
}

// C batches through multi-insert slots; per-row inserts write the same page
// image.
pub fn copyTemplateDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    templateDbId: Oid,
    newDbId: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;
    let mut indstate = None;

    let keys = [oid_key(Anum_pg_shdepend_dbid, templateDbId)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SharedDependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let td = rel.descr();
        let values = [
            Datum::from_oid(newDbId),
            getattr(tup, Anum_pg_shdepend_classid, td),
            getattr(tup, Anum_pg_shdepend_objid, td),
            getattr(tup, Anum_pg_shdepend_objsubid, td),
            getattr(tup, Anum_pg_shdepend_refclassid, td),
            getattr(tup, Anum_pg_shdepend_refobjid, td),
            getattr(tup, Anum_pg_shdepend_deptype, td),
        ];
        let nulls = [false; Natts_pg_shdepend];
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &rel)?);
        }
        let mut copy = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsertWithInfo(
            mcx,
            &rel,
            &mut copy,
            indstate.as_mut().unwrap(),
        )?;
    }
    genam::systable_endscan(mcx, scan)?;

    if let Some(st) = indstate {
        catalog_indexing::CatalogCloseIndexes(st)?;
    }
    rel.close(RowExclusiveLock)
}

pub fn dropDatabaseDependencies<'mcx>(mcx: Mcx<'mcx>, databaseId: Oid) -> PgResult<()> {
    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;

    let keys = [oid_key(Anum_pg_shdepend_dbid, databaseId)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &sdepRel,
        catalog::SharedDependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&sdepRel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;

    shdepDropDependency(
        mcx,
        &sdepRel,
        DATABASE_RELATION_ID,
        databaseId,
        0,
        true,
        InvalidOid,
        InvalidOid,
        None,
    )?;

    sdepRel.close(RowExclusiveLock)
}

pub fn deleteSharedDependencyRecordsFor<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    objectSubId: i32,
) -> PgResult<()> {
    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;

    shdepDropDependency(
        mcx,
        &sdepRel,
        classId,
        objectId,
        objectSubId,
        objectSubId == 0,
        InvalidOid,
        InvalidOid,
        None,
    )?;

    sdepRel.close(RowExclusiveLock)
}

fn shdepAddDependency<'mcx>(
    mcx: Mcx<'mcx>,
    sdepRel: &Relation<'mcx>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    refclassId: Oid,
    refobjId: Oid,
    deptype: SharedDependencyType,
) -> PgResult<()> {
    shdepLockAndCheckObject(mcx, refclassId, refobjId)?;

    let values = [
        Datum::from_oid(classIdGetDbId(classId)),
        Datum::from_oid(classId),
        Datum::from_oid(objectId),
        Datum::from_i32(objsubId),
        Datum::from_oid(refclassId),
        Datum::from_oid(refobjId),
        Datum::from_char(deptype.as_char()),
    ];
    let nulls = [false; Natts_pg_shdepend];
    let mut tup = heaptuple::heap_form_tuple(mcx, sdepRel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, sdepRel, &mut tup)
}

// deptype None is the C SHARED_DEPENDENCY_INVALID wildcard.
fn shdepDropDependency<'mcx>(
    mcx: Mcx<'mcx>,
    sdepRel: &Relation<'mcx>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    drop_subobjects: bool,
    refclassId: Oid,
    refobjId: Oid,
    deptype: Option<SharedDependencyType>,
) -> PgResult<()> {
    let keys = [
        oid_key(Anum_pg_shdepend_dbid, classIdGetDbId(classId)),
        oid_key(Anum_pg_shdepend_classid, classId),
        oid_key(Anum_pg_shdepend_objid, objectId),
        int4_key(Anum_pg_shdepend_objsubid, objsubId),
    ];
    let nkeys = if drop_subobjects { 3 } else { 4 };

    let desc = sdepRel.descr();
    let mut scan = genam::systable_beginscan(
        mcx,
        sdepRel,
        catalog::SharedDependDependerIndexId,
        true,
        None,
        &keys[..nkeys],
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let form = form_pg_shdepend(tup, desc);
        if OidIsValid(refclassId) && form.refclassid != refclassId {
            continue;
        }
        if OidIsValid(refobjId) && form.refobjid != refobjId {
            continue;
        }
        if let Some(dt) = deptype {
            if form.deptype != dt.as_char() {
                continue;
            }
        }
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(sdepRel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)
}

fn classIdGetDbId(classId: Oid) -> Oid {
    if catalog::IsSharedRelation(classId) {
        InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    }
}

pub fn shdepLockAndCheckObject<'mcx>(mcx: Mcx<'mcx>, classId: Oid, objectId: Oid) -> PgResult<()> {
    lmgr::LockSharedObject(classId, objectId, 0, AccessShareLock)?;

    if classId == AUTH_ID_RELATION_ID {
        if syscache_seams::lookup_authid_rolname::call(mcx, objectId)?.is_none() {
            return Err(Box::new(
                PgError::error(format!("role {objectId} was concurrently dropped"))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
    } else if classId == TABLE_SPACE_RELATION_ID {
        if tablespace_seams::get_tablespace_name::call(mcx, objectId)?.is_none() {
            return Err(Box::new(
                PgError::error(format!("tablespace {objectId} was concurrently dropped"))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
    } else if classId == DATABASE_RELATION_ID {
        if dbcommands_seams::get_database_name::call(objectId)?.is_none() {
            return Err(Box::new(
                PgError::error(format!("database {objectId} was concurrently dropped"))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
    } else {
        return Err(Box::new(PgError::error(format!(
            "unrecognized shared classId: {classId}"
        ))));
    }

    Ok(())
}

fn storeObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    descs: &mut PgString<'mcx>,
    obj: &ShDependObjectInfo,
) -> PgResult<()> {
    let objdesc = getObjectDescription(mcx, obj.classId, obj.objectId, obj.objectSubId)?;

    if !descs.is_empty() {
        descs.try_push('\n')?;
    }

    let prefixed = match obj.deptype as u8 {
        b'o' => format!("owner of {objdesc}"),
        b'a' => format!("privileges for {objdesc}"),
        b'i' => format!("initial privileges for {objdesc}"),
        b'r' => format!("target of {objdesc}"),
        b't' => format!("tablespace for {objdesc}"),
        _ => {
            return Err(Box::new(PgError::error(format!(
                "unrecognized dependency type: {}",
                obj.deptype as i32
            ))))
        }
    };
    descs.try_push_str(&prefixed)
}

fn storeRemoteObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    descs: &mut PgString<'mcx>,
    dbOid: Oid,
    count: i32,
) -> PgResult<()> {
    let objdesc = getObjectDescription(mcx, DATABASE_RELATION_ID, dbOid, 0)?;

    if !descs.is_empty() {
        descs.try_push('\n')?;
    }

    let plural = if count == 1 { "object" } else { "objects" };
    descs.try_push_str(&format!("{count} {plural} in {objdesc}"))
}

// getObjectDescription (objectaddress.c) lives in catalog_objectaddress;
// seam because pg_depend (a dep of this crate) sits below it.
fn getObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    objectSubId: i32,
) -> PgResult<String> {
    Ok(
        objectaddress_seams::get_object_description::call(
            mcx, classId, objectId, objectSubId, false,
        )?
        .expect("missing_ok=false"),
    )
}

pub fn shdepDropOwned<'mcx>(
    mcx: Mcx<'mcx>,
    roleids: &[Oid],
    behavior: DropBehavior,
) -> PgResult<()> {
    let mut deleteobjs: PgVec<'mcx, (Oid, Oid, i32)> = PgVec::new_in(mcx);

    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;
    let desc = sdepRel.descr();
    let myDatabaseId = init_small::globals::MyDatabaseId();

    for &roleid in roleids {
        if catalog::IsPinnedObject(AUTH_ID_RELATION_ID, roleid) {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot drop objects owned by {} because they are required by the database system",
                    getObjectDescription(mcx, AUTH_ID_RELATION_ID, roleid, 0)?
                ))
                .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            ));
        }

        let keys = [
            oid_key(Anum_pg_shdepend_refclassid, AUTH_ID_RELATION_ID),
            oid_key(Anum_pg_shdepend_refobjid, roleid),
        ];
        let mut scan = genam::systable_beginscan(
            mcx,
            &sdepRel,
            catalog::SharedDependReferenceIndexId,
            true,
            None,
            &keys,
        )?;
        loop {
            let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else { break };
            // SAFETY: aliases the slot-held image for the recheck call below.
            let view = unsafe {
                HeapTupleData::from_raw_parts(
                    tup.header_ptr(),
                    tup.t_len,
                    tup.t_self,
                    tup.t_tableOid,
                )
            };
            let tup = &view;
            let form = form_pg_shdepend(tup, desc);

            if form.dbid != myDatabaseId && form.dbid != InvalidOid {
                continue;
            }

            match form.deptype as u8 {
                b'r' => {
                    if !policy_seams::remove_role_from_object_policy::call(
                        mcx,
                        roleid,
                        form.classid,
                        form.objid,
                    )? {
                        acquire_deletion_lock::call(form.classid, form.objid, 0)?;
                        if !genam::systable_recheck_tuple(mcx, &mut scan, tup)? {
                            release_deletion_lock::call(form.classid, form.objid)?;
                            continue;
                        }
                        deleteobjs.push((form.classid, form.objid, form.objsubid));
                    }
                }
                // Role-grant ACL rows (pg_auth_members) drop the whole row:
                // fall through to the OWNER arm, as in C.
                b'a' if form.classid != catalog::AuthMemRelationId => {
                    remove_role_from_object_acl::call(mcx, roleid, form.classid, form.objid)?;
                }
                b'a' | b'o' => {
                    if form.dbid == myDatabaseId || form.classid == catalog::AuthMemRelationId {
                        acquire_deletion_lock::call(form.classid, form.objid, 0)?;
                        if !genam::systable_recheck_tuple(mcx, &mut scan, tup)? {
                            release_deletion_lock::call(form.classid, form.objid)?;
                            continue;
                        }
                        deleteobjs.push((form.classid, form.objid, form.objsubid));
                    }
                }
                b'i' => {
                    debug_assert!(form.classid != catalog::AuthMemRelationId);
                    remove_role_from_init_priv::call(
                        mcx,
                        roleid,
                        form.classid,
                        form.objid,
                        form.objsubid,
                    )?;
                }
                _ => {
                    return Err(Box::new(PgError::error(
                        "unexpected dependency type".to_string(),
                    )))
                }
            }
        }
        genam::systable_endscan(mcx, scan)?;
    }

    perform_multiple_deletions::call(mcx, &deleteobjs, behavior, 0)?;

    sdepRel.close(RowExclusiveLock)
}

// Classids of shdepReassignOwned_Owner's switch not covered by the
// types_core/catalog constant sets.
const ConversionRelationId: Oid = 2607;
const EventTriggerRelationId: Oid = 3466;
const PublicationRelationId: Oid = 6104;
const StatisticExtRelationId: Oid = 3381;
const TSConfigRelationId: Oid = 3602;
const TSDictionaryRelationId: Oid = 3600;
const DefaultAclRelationId: Oid = 826;

pub fn shdepReassignOwned<'mcx>(mcx: Mcx<'mcx>, roleids: &[Oid], newrole: Oid) -> PgResult<()> {
    let sdepRel = table::table_open(mcx, catalog::SharedDependRelationId, RowExclusiveLock)?;
    let desc = sdepRel.descr();
    let myDatabaseId = init_small::globals::MyDatabaseId();

    for &roleid in roleids {
        if catalog::IsPinnedObject(AUTH_ID_RELATION_ID, roleid) {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot reassign ownership of objects owned by {} because they are required by the database system",
                    getObjectDescription(mcx, AUTH_ID_RELATION_ID, roleid, 0)?
                ))
                .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            ));
        }

        let keys = [
            oid_key(Anum_pg_shdepend_refclassid, AUTH_ID_RELATION_ID),
            oid_key(Anum_pg_shdepend_refobjid, roleid),
        ];
        let mut scan = genam::systable_beginscan(
            mcx,
            &sdepRel,
            catalog::SharedDependReferenceIndexId,
            true,
            None,
            &keys,
        )?;
        loop {
            let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else { break };
            let form = form_pg_shdepend(tup, desc);

            if form.dbid != myDatabaseId && form.dbid != InvalidOid {
                continue;
            }

            // C runs each callee under a short-lived AllocSet purely to bound
            // leakage across many objects; allocation lifetime only, elided.
            match form.deptype as u8 {
                b'o' => shdepReassignOwned_Owner(mcx, &form, newrole)?,
                b'i' => {
                    replace_role_in_init_priv::call(
                        mcx,
                        roleid,
                        newrole,
                        form.classid,
                        form.objid,
                        form.objsubid,
                    )?;
                }
                b'a' | b'r' | b't' => {}
                other => {
                    return Err(Box::new(PgError::error(format!(
                        "unrecognized dependency type: {}",
                        other as i32
                    ))));
                }
            }

            command_counter_increment::call()?;
        }
        genam::systable_endscan(mcx, scan)?;
    }

    sdepRel.close(RowExclusiveLock)
}

fn shdepReassignOwned_Owner<'mcx>(
    mcx: Mcx<'mcx>,
    sdepForm: &FormPgShdepend,
    newrole: Oid,
) -> PgResult<()> {
    match sdepForm.classid {
        types_core::TYPE_RELATION_ID => {
            alter_type_owner_oid::call(mcx, sdepForm.objid, newrole, true)
        }
        NAMESPACE_RELATION_ID => alter_schema_owner_oid::call(mcx, sdepForm.objid, newrole),
        // recursing=true so indexes/owned sequences visited before their
        // parent table don't fail.
        RELATION_RELATION_ID => {
            at_exec_change_owner::call(mcx, sdepForm.objid, newrole, true, AccessExclusiveLock)
        }
        // Default ACLs and user mappings are DROP OWNED's problem, not
        // REASSIGN OWNED's.
        DefaultAclRelationId | types_core::USER_MAPPING_RELATION_ID => Ok(()),
        types_core::FOREIGN_SERVER_RELATION_ID => {
            alter_foreign_server_owner_oid::call(mcx, sdepForm.objid, newrole)
        }
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => {
            alter_foreign_data_wrapper_owner_oid::call(mcx, sdepForm.objid, newrole)
        }
        EventTriggerRelationId => alter_event_trigger_owner_oid::call(mcx, sdepForm.objid, newrole),
        PublicationRelationId => alter_publication_owner_oid::call(mcx, sdepForm.objid, newrole),
        catalog::SubscriptionRelationId => {
            alter_subscription_owner_oid::call(mcx, sdepForm.objid, newrole)
        }
        catalog::CollationRelationId
        | ConversionRelationId
        | types_core::OPERATOR_RELATION_ID
        | types_core::PROCEDURE_RELATION_ID
        | types_core::LANGUAGE_RELATION_ID
        | catalog::LargeObjectRelationId
        | types_core::OPERATOR_FAMILY_RELATION_ID
        | catalog::OperatorClassRelationId
        | types_core::EXTENSION_RELATION_ID
        | StatisticExtRelationId
        | TABLE_SPACE_RELATION_ID
        | DATABASE_RELATION_ID
        | TSConfigRelationId
        | TSDictionaryRelationId => {
            alter_object_owner_internal::call(mcx, sdepForm.classid, sdepForm.objid, newrole)
        }
        other => Err(Box::new(PgError::error(format!(
            "unexpected classid {other}"
        )))),
    }
}
