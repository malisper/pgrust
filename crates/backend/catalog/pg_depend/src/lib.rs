// pg_depend.c recording slice; the deletion half rides in catalog_dependency
// (deleteOneObject's scans); the pg_shdepend.c wrappers delegate to the
// pg_shdepend crate.
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

const RELATION_CLASS: Oid = types_core::RELATION_RELATION_ID;
const TYPE_CLASS: Oid = types_core::TYPE_RELATION_ID;
const PROC_CLASS: Oid = 1255;
const OPER_CLASS: Oid = 2617;
const COLL_CLASS: Oid = 3456;
const DEFAULT_COLLATION_OID: Oid = 100;

struct FindExprRefs<'a, 'mcx> {
    rel_id: Oid,
    addrs: &'a mut mcx::PgVec<'mcx, ObjectAddress>,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for FindExprRefs<'_, 'mcx> {
    fn visit(&mut self, node: types_nodes::Node<'mcx>) -> PgResult<bool> {
        use types_nodes::NodeTag::*;
        let addrs = &mut *self.addrs;
        let rel_id = self.rel_id;
        match node.node_tag() {
            T_Var => {
                let v = node.as_var().expect("Var");
                assert!(
                    v.varlevelsup == 0 && v.varno == 1,
                    "find_expr_references_walker (dependency.c): var beyond the \
                     single-rel rtable; unported lane"
                );
                if v.varattno != 0 {
                    addrs.push(ObjectAddress::sub_set(RELATION_CLASS, rel_id, v.varattno as i32));
                }
                return Ok(false);
            }
            T_Const => {
                let c = node.as_const().expect("Const");
                addrs.push(ObjectAddress::set(TYPE_CLASS, c.consttype));
                if c.constcollid != 0 && c.constcollid != DEFAULT_COLLATION_OID {
                    addrs.push(ObjectAddress::set(COLL_CLASS, c.constcollid));
                }
                const REG_TYPES: [Oid; 11] =
                    [24, 2202, 2203, 2204, 2205, 2206, 4191, 3734, 3769, 4089, 4096];
                assert!(
                    !REG_TYPES.contains(&c.consttype),
                    "find_expr_references_walker (dependency.c): reg* literal; unported lane"
                );
                return Ok(false);
            }
            T_Param => {
                let p = node.as_variant::<types_nodes::primnodes::Param>().expect("Param");
                addrs.push(ObjectAddress::set(TYPE_CLASS, p.paramtype));
                if p.paramcollid != 0 && p.paramcollid != DEFAULT_COLLATION_OID {
                    addrs.push(ObjectAddress::set(COLL_CLASS, p.paramcollid));
                }
            }
            T_FuncExpr => {
                addrs.push(ObjectAddress::set(
                    PROC_CLASS,
                    node.as_func_expr().expect("FuncExpr").funcid,
                ));
            }
            T_OpExpr => {
                addrs.push(ObjectAddress::set(
                    OPER_CLASS,
                    node.as_op_expr().expect("OpExpr").opno,
                ));
            }
            T_DistinctExpr => {
                addrs.push(ObjectAddress::set(
                    OPER_CLASS,
                    node.as_distinct_expr().expect("DistinctExpr").opno,
                ));
            }
            T_ScalarArrayOpExpr => {
                addrs.push(ObjectAddress::set(
                    OPER_CLASS,
                    node.as_scalar_array_op_expr().expect("SAOP").opno,
                ));
            }
            T_RelabelType => {
                let r = node.as_relabel_type().expect("RelabelType");
                addrs.push(ObjectAddress::set(TYPE_CLASS, r.resulttype));
                if r.resultcollid != 0 && r.resultcollid != DEFAULT_COLLATION_OID {
                    addrs.push(ObjectAddress::set(COLL_CLASS, r.resultcollid));
                }
            }
            T_CoerceViaIO => {
                let c = node
                    .as_variant::<types_nodes::primnodes::CoerceViaIO>()
                    .expect("CoerceViaIO");
                addrs.push(ObjectAddress::set(TYPE_CLASS, c.resulttype));
                if c.resultcollid != 0 && c.resultcollid != DEFAULT_COLLATION_OID {
                    addrs.push(ObjectAddress::set(COLL_CLASS, c.resultcollid));
                }
            }
            T_BoolExpr | T_NullTest | T_BooleanTest | T_CaseExpr | T_CaseWhen
            | T_CaseTestExpr | T_CoalesceExpr | T_MinMaxExpr | T_ArrayExpr | T_List => {}
            other => panic!(
                "find_expr_references_walker (dependency.c): {other:?}; unported lane"
            ),
        }
        nodes_core::expression_tree_walker(node, self)
    }
}

// recordDependencyOnSingleRelExpr (dependency.c), reverse_self=false lane over
// the committed expression node set.
pub fn recordDependencyOnSingleRelExpr<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &ObjectAddress,
    expr: types_nodes::Node<'mcx>,
    rel_id: Oid,
    behavior: DependencyType,
    self_behavior: DependencyType,
) -> PgResult<()> {
    let mut addrs: mcx::PgVec<'mcx, ObjectAddress> = mcx::PgVec::new_in(mcx);
    nodes_core::NodeWalker::visit(&mut FindExprRefs { rel_id, addrs: &mut addrs }, expr)?;
    eliminate_duplicate_dependencies(&mut addrs);

    if behavior != self_behavior && !addrs.is_empty() {
        let mut self_addrs: mcx::PgVec<'mcx, ObjectAddress> = mcx::PgVec::new_in(mcx);
        let mut rest: mcx::PgVec<'mcx, ObjectAddress> = mcx::PgVec::new_in(mcx);
        for a in addrs.iter() {
            if a.classId == RELATION_CLASS && a.objectId == rel_id {
                self_addrs.push(*a);
            } else {
                rest.push(*a);
            }
        }
        recordMultipleDependencies(mcx, depender, &self_addrs, self_behavior)?;
        return recordMultipleDependencies(mcx, depender, &rest, behavior);
    }
    recordMultipleDependencies(mcx, depender, &addrs, behavior)
}

// eliminate_duplicate_dependencies (dependency.c): sort, drop identicals; a
// whole-object ref (subId 0 sorts first) collapses into the first column ref
// of the same object that follows it.
fn eliminate_duplicate_dependencies(addrs: &mut mcx::PgVec<'_, ObjectAddress>) {
    if addrs.len() <= 1 {
        return;
    }
    addrs.sort_by(object_address_comparator);
    let mut kept = 1;
    for i in 1..addrs.len() {
        let this = addrs[i];
        let prior = addrs[kept - 1];
        if prior.classId == this.classId && prior.objectId == this.objectId {
            if prior.objectSubId == this.objectSubId {
                continue;
            }
            if prior.objectSubId == 0 {
                addrs[kept - 1].objectSubId = this.objectSubId;
                continue;
            }
        }
        addrs[kept] = this;
        kept += 1;
    }
    addrs.truncate(kept);
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

// changeDependencyFor (pg_depend.c).
pub fn changeDependencyFor<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> PgResult<i64> {
    let old_is_pinned = isObjectPinned(&ObjectAddress::set(refClassId, oldRefObjectId));
    let new_is_pinned = isObjectPinned(&ObjectAddress::set(refClassId, newRefObjectId));
    if old_is_pinned {
        if new_is_pinned {
            return Ok(1);
        }
        recordDependencyOn(
            mcx,
            &ObjectAddress::set(classId, objectId),
            &ObjectAddress::set(refClassId, newRefObjectId),
            DependencyType::Normal,
        )?;
        return Ok(1);
    }
    let mut count = 0i64;
    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let keys = [oid_key(Anum_pg_depend_classid, classId), oid_key(Anum_pg_depend_objid, objectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    let desc = rel.descr();
    let natts = desc.natts as usize;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refclassid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_depend_refclassid as i32, desc, &mut isnull)
        }
        .as_oid();
        // SAFETY: as above.
        let refobjid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_depend_refobjid as i32, desc, &mut isnull)
        }
        .as_oid();
        if refclassid != refClassId || refobjid != oldRefObjectId {
            continue;
        }
        let tid = tup.t_self;
        if new_is_pinned {
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
        } else {
            let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            values[Anum_pg_depend_refobjid - 1] = Datum::from_oid(newRefObjectId);
            replace[Anum_pg_depend_refobjid - 1] = true;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &rel, &tid, &mut newtup)?;
        }
        count += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)?;
    Ok(count)
}

// creating_extension / CurrentExtensionObject (extension.c:79-80) are hosted
// here, one layer below their C home: extension depends on this crate, and
// recordDependencyOnCurrentExtension reads them per row.
thread_local! {
    static CREATING_EXTENSION: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
    static CURRENT_EXTENSION_OBJECT: core::cell::Cell<Oid> =
        const { core::cell::Cell::new(types_core::InvalidOid) };
}

pub fn creating_extension() -> bool {
    CREATING_EXTENSION.with(|c| c.get())
}

pub fn CurrentExtensionObject() -> Oid {
    CURRENT_EXTENSION_OBJECT.with(|c| c.get())
}

pub fn set_creating_extension(v: bool) {
    CREATING_EXTENSION.with(|c| c.set(v));
}

pub fn set_current_extension_object(oid: Oid) {
    CURRENT_EXTENSION_OBJECT.with(|c| c.set(oid));
}

pub fn getExtensionOfObject<'mcx>(mcx: Mcx<'mcx>, classId: Oid, objectId: Oid) -> PgResult<Oid> {
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [oid_key(Anum_pg_depend_classid, classId), oid_key(Anum_pg_depend_objid, objectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    let mut result = types_core::InvalidOid;
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
            == types_core::EXTENSION_RELATION_ID
            && dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8()
                == DependencyType::Extension.as_char()
        {
            result = dep_attr(&view, Anum_pg_depend_refobjid, desc).as_oid();
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

pub fn recordDependencyOnCurrentExtension<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    is_replace: bool,
) -> PgResult<()> {
    debug_assert!(object.objectSubId == 0);

    if !creating_extension() {
        return Ok(());
    }

    if is_replace {
        let oldext = getExtensionOfObject(mcx, object.classId, object.objectId)?;
        if oldext != types_core::InvalidOid {
            if oldext == CurrentExtensionObject() {
                return Ok(());
            }
            // The 55000 report needs getObjectDescription (objectaddress lane).
            panic!(
                "recordDependencyOnCurrentExtension (pg_depend.c): object \
                 {}/{} is already a member of extension {oldext}",
                object.classId, object.objectId
            );
        }
        panic!(
            "recordDependencyOnCurrentExtension (pg_depend.c): free-standing object \
             {}/{} replaced by extension {} (needs getObjectDescription for the 55000 report)",
            object.classId,
            object.objectId,
            CurrentExtensionObject()
        );
    }

    let extension = ObjectAddress::set(types_core::EXTENSION_RELATION_ID, CurrentExtensionObject());
    recordDependencyOn(mcx, object, &extension, DependencyType::Extension)
}

pub fn recordDependencyOnOwner<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    owner: Oid,
) -> PgResult<()> {
    pg_shdepend::recordDependencyOnOwner(mcx, classId, objectId, owner)
}

pub fn updateAclDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    objsubId: i32,
    ownerId: Oid,
    oldmembers: &[Oid],
    newmembers: &[Oid],
) -> PgResult<()> {
    pg_shdepend::updateAclDependencies(
        mcx, classId, objectId, objsubId, ownerId, oldmembers, newmembers,
    )
}

const Anum_pg_depend_classid: usize = 1;
const Anum_pg_depend_objid: usize = 2;
const Anum_pg_depend_objsubid: usize = 3;
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

fn int4_key(attno: usize, v: i32) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT4EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(int4eq) failed: {e:?}"));
    key.sk_argument = Datum::from_i32(v);
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

// getIdentitySequence (pg_depend.c) over getOwnedSequences_internal: the
// INTERNAL pg_depend edge from the sequence to (relid, attnum). DIVERGENCE:
// C also probes get_rel_relkind == RELKIND_SEQUENCE; INTERNAL deps of a
// column from pg_class are only identity sequences in every ported lane.
pub fn getIdentitySequence<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i32,
    missing_ok: bool,
) -> PgResult<Oid> {
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_depend_refclassid, types_core::RELATION_RELATION_ID),
        oid_key(Anum_pg_depend_refobjid, relid),
        int4_key(Anum_pg_depend_refobjsubid, attnum),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependReferenceIndexId, true, None, &keys)?;
    let mut result = types_core::InvalidOid;
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
        if dep_attr(&view, Anum_pg_depend_classid, desc).as_oid()
            == types_core::RELATION_RELATION_ID
            && dep_attr(&view, Anum_pg_depend_objsubid, desc).as_i32() == 0
            && dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8()
                == DependencyType::Internal.as_char()
        {
            if result != types_core::InvalidOid {
                panic!("more than one owned sequence found for column {relid}.{attnum}");
            }
            result = dep_attr(&view, Anum_pg_depend_objid, desc).as_oid();
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    if result == types_core::InvalidOid && !missing_ok {
        panic!("no owned sequence found for identity column {relid}.{attnum}");
    }
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
