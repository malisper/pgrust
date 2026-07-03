// pg_depend.c recording slice; deletion/scan half and pg_shdepend unported.
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

fn object_address_comparator(a: &ObjectAddress, b: &ObjectAddress) -> core::cmp::Ordering {
    b.objectId
        .cmp(&a.objectId)
        .then(a.classId.cmp(&b.classId))
        .then((a.objectSubId as u32).cmp(&(b.objectSubId as u32)))
}

fn isObjectPinned(object: &ObjectAddress) -> bool {
    catalog::IsPinnedObject(object.classId, object.objectId)
}

// recordDependencyOnOwner (pg_shdepend.c): a pinned owner records nothing;
// pg_shdepend writes are unported, so any unpinned owner is loud.
pub fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid) {
    if !catalog::IsPinnedObject(types_core::AUTH_ID_RELATION_ID, owner) {
        panic!(
            "recordDependencyOnOwner (pg_shdepend.c): pg_shdepend recording unported \
             (class {classId} object {objectId} owner {owner})"
        );
    }
}
