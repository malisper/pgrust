// pg_depend.c recording slice; the deletion half rides in catalog_dependency
// (deleteOneObject's scans); pg_shdepend writes unported.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_rel::RowExclusiveLock;

pub const DependRelationId: Oid = 2608;
pub const DependDependerIndexId: Oid = 2673;
pub const DependReferenceIndexId: Oid = 2674;

const Natts_pg_depend: usize = 7;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: i32,
}

impl ObjectAddress {
    pub const fn set(classId: Oid, objectId: Oid) -> Self {
        Self { classId, objectId, objectSubId: 0 }
    }

    pub const fn sub_set(classId: Oid, objectId: Oid, objectSubId: i32) -> Self {
        Self { classId, objectId, objectSubId }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DependencyType {
    Normal,
    Auto,
    Internal,
    PartitionPri,
    PartitionSec,
    Extension,
    AutoExtension,
}

impl DependencyType {
    pub const fn as_char(self) -> i8 {
        (match self {
            DependencyType::Normal => b'n',
            DependencyType::Auto => b'a',
            DependencyType::Internal => b'i',
            DependencyType::PartitionPri => b'P',
            DependencyType::PartitionSec => b'S',
            DependencyType::Extension => b'e',
            DependencyType::AutoExtension => b'x',
        }) as i8
    }
}

pub fn recordDependencyOn<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &ObjectAddress,
    referenced: &ObjectAddress,
    behavior: DependencyType,
) -> PgResult<()> {
    recordMultipleDependencies(mcx, depender, core::slice::from_ref(referenced), behavior)
}

pub fn recordMultipleDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &ObjectAddress,
    referenced: &[ObjectAddress],
    behavior: DependencyType,
) -> PgResult<()> {
    if referenced.is_empty() {
        return Ok(());
    }
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let mut indstate = None;
    for r in referenced {
        if isObjectPinned(r) {
            continue;
        }
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &rel)?);
        }
        // C batches through multi-insert slots; per-row inserts write the same
        // page image (WAL record shape differs).
        let values = [
            Datum::from_oid(depender.classId),
            Datum::from_oid(depender.objectId),
            Datum::from_i32(depender.objectSubId),
            Datum::from_oid(r.classId),
            Datum::from_oid(r.objectId),
            Datum::from_i32(r.objectSubId),
            Datum::from_char(behavior.as_char()),
        ];
        let nulls = [false; Natts_pg_depend];
        let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
        catalog_indexing::CatalogTupleInsertWithInfo(
            mcx,
            &rel,
            &mut tup,
            indstate.as_mut().unwrap(),
        )?;
    }
    if let Some(st) = indstate {
        catalog_indexing::CatalogCloseIndexes(st)?;
    }
    rel.close(RowExclusiveLock)
}

// record_object_address_dependencies (dependency.c): sort + dedup, then record.
pub fn record_object_address_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &ObjectAddress,
    referenced: &mut [ObjectAddress],
    behavior: DependencyType,
) -> PgResult<()> {
    referenced.sort_by(object_address_comparator);
    let mut kept = 0;
    for i in 0..referenced.len() {
        if kept == 0 || referenced[i] != referenced[kept - 1] {
            referenced[kept] = referenced[i];
            kept += 1;
        }
    }
    recordMultipleDependencies(mcx, depender, &referenced[..kept], behavior)
}

pub fn object_address_comparator(a: &ObjectAddress, b: &ObjectAddress) -> core::cmp::Ordering {
    b.objectId
        .cmp(&a.objectId)
        .then(a.classId.cmp(&b.classId))
        .then((a.objectSubId as u32).cmp(&(b.objectSubId as u32)))
}

fn isObjectPinned(object: &ObjectAddress) -> bool {
    catalog::IsPinnedObject(object.classId, object.objectId)
}

// deleteDependencyRecordsFor (pg_depend.c).
pub fn deleteDependencyRecordsFor<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    skipExtensionDeps: bool,
) -> PgResult<i64> {
    const Anum_pg_depend_classid: usize = 1;
    const Anum_pg_depend_objid: usize = 2;
    const Anum_pg_depend_deptype: i32 = 7;
    const DEPENDENCY_EXTENSION: i8 = b'e' as i8;

    let mut count = 0i64;
    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let key = |attno: usize, oid: Oid| -> types_scan::scankey::ScanKeyData {
        let mut k = types_scan::scankey::ScanKeyData::empty();
        k.sk_attno = attno as types_core::AttrNumber;
        k.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
        k.sk_collation = 0;
        k.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
            .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
        k.sk_argument = Datum::from_oid(oid);
        k
    };
    let keys = [key(Anum_pg_depend_classid, classId), key(Anum_pg_depend_objid, objectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        if skipExtensionDeps {
            let mut isnull = false;
            // SAFETY: deptype is a fixed NOT NULL pg_depend column.
            let deptype = unsafe {
                types_tuple::heap_getattr(tup, Anum_pg_depend_deptype, rel.descr(), &mut isnull)
            }
            .as_i8();
            if deptype == DEPENDENCY_EXTENSION {
                continue;
            }
        }
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
        count += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)?;
    Ok(count)
}

// A pinned owner records nothing; pg_shdepend writes unported → loud.
pub fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) {
    if !catalog::IsPinnedObject(types_core::AUTH_ID_RELATION_ID, owner) {
        panic!(
            "recordDependencyOnOwner (pg_shdepend.c): pg_shdepend recording unported \
             (class {classId} object {objectId} owner {owner})"
        );
    }
}

// SHARED_DEPENDENCY_ACL rows never cover PUBLIC, the owner, or pinned roles,
// so those grants need no pg_shdepend writes; any other role is loud.
pub fn updateAclDependencies(
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    ownerId: Oid,
    oldmembers: &[Oid],
    newmembers: &[Oid],
) {
    let check = |roleid: Oid, other: &[Oid]| {
        if other.contains(&roleid)
            || roleid == ownerId
            || catalog::IsPinnedObject(types_core::AUTH_ID_RELATION_ID, roleid)
        {
            return;
        }
        panic!(
            "updateAclDependencies (pg_shdepend.c): pg_shdepend recording unported \
             (class {classId} object {objectId} subid {objsubId} role {roleid})"
        );
    };
    for &r in newmembers {
        check(r, oldmembers);
    }
    for &r in oldmembers {
        check(r, newmembers);
    }
}

const Anum_pg_depend_classid: usize = 1;
const Anum_pg_depend_objid: usize = 2;
const Anum_pg_depend_refclassid: usize = 4;
const Anum_pg_depend_refobjid: usize = 5;
const Anum_pg_depend_refobjsubid: usize = 6;
const Anum_pg_depend_deptype: usize = 7;

fn oid_key(attno: usize, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(oideq) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

fn dep_attr(
    tup: &types_tuple::HeapTupleData<'_>,
    attnum: usize,
    desc: &types_tuple::TupleDescData<'_>,
) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_depend column under the relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

// sequenceIsOwned: Some((table_relid, attnum)) iff a pg_depend row records
// (RelationRelationId, seqId, 0) -> (RelationRelationId, ., .) with deptype.
pub fn sequenceIsOwned<'mcx>(
    mcx: Mcx<'mcx>,
    seqId: Oid,
    deptype: DependencyType,
) -> PgResult<Option<(Oid, i32)>> {
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_depend_classid, types_core::RELATION_RELATION_ID),
        oid_key(Anum_pg_depend_objid, seqId),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    let mut result = None;
    let desc = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        // SAFETY: aliases the slot-held image for this iteration's reads only.
        let view = unsafe {
            types_tuple::HeapTupleData::from_raw_parts(
                tup.header_ptr().cast_mut(),
                tup.t_len,
                tup.t_self,
                tup.t_tableOid,
            )
        };
        if dep_attr(&view, Anum_pg_depend_refclassid, desc).as_oid()
            == types_core::RELATION_RELATION_ID
            && dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8() == deptype.as_char()
        {
            result = Some((
                dep_attr(&view, Anum_pg_depend_refobjid, desc).as_oid(),
                dep_attr(&view, Anum_pg_depend_refobjsubid, desc).as_i32(),
            ));
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

pub fn deleteDependencyRecordsForClass<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    refclassId: Oid,
    deptype: DependencyType,
) -> PgResult<i64> {
    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let keys = [oid_key(Anum_pg_depend_classid, classId), oid_key(Anum_pg_depend_objid, objectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    let mut count = 0i64;
    let desc = rel.descr();
    loop {
        let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else { break };
        let tid = tup.t_self;
        // SAFETY: aliases the slot-held image for this iteration's reads only.
        let view = unsafe {
            types_tuple::HeapTupleData::from_raw_parts(
                tup.header_ptr().cast_mut(),
                tup.t_len,
                tup.t_self,
                tup.t_tableOid,
            )
        };
        if dep_attr(&view, Anum_pg_depend_refclassid, desc).as_oid() == refclassId
            && dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8() == deptype.as_char()
        {
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
            count += 1;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)?;
    Ok(count)
}

// get_index_constraint: the index's internal-dependency constraint, or InvalidOid.
pub fn get_index_constraint<'mcx>(mcx: Mcx<'mcx>, index_id: Oid) -> PgResult<Oid> {
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    const ConstraintRelationId: Oid = 2606;
    let mut keys = [ScanKeyData::empty(), ScanKeyData::empty(), ScanKeyData::empty()];
    let fns = [
        (1u16, types_core::fmgr::F_OIDEQ, Datum::from_oid(types_core::RELATION_RELATION_ID)),
        (2u16, types_core::fmgr::F_OIDEQ, Datum::from_oid(index_id)),
        (3u16, types_core::fmgr::F_INT4EQ, Datum::from_i32(0)),
    ];
    for (k, (attno, f, arg)) in keys.iter_mut().zip(fns) {
        k.sk_attno = attno as types_core::AttrNumber;
        k.sk_strategy = BTEqualStrategyNumber;
        k.sk_collation = 0;
        k.sk_func = fmgr_seams::fmgr_info::call(f)
            .unwrap_or_else(|e| panic!("fmgr_info({f}) failed: {e:?}"));
        k.sk_argument = arg;
    }
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let mut scan =
        genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    let mut constraint_id = types_core::InvalidOid;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refclassid =
            unsafe { types_tuple::heap_getattr(tup, 4, rel.descr(), &mut isnull) }.as_oid();
        let refobjid =
            unsafe { types_tuple::heap_getattr(tup, 5, rel.descr(), &mut isnull) }.as_oid();
        let refobjsubid =
            unsafe { types_tuple::heap_getattr(tup, 6, rel.descr(), &mut isnull) }.as_i32();
        let deptype =
            unsafe { types_tuple::heap_getattr(tup, 7, rel.descr(), &mut isnull) }.as_i8() as u8;
        if refclassid == ConstraintRelationId && refobjsubid == 0 && deptype == b'i' {
            constraint_id = refobjid;
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(constraint_id)
}
