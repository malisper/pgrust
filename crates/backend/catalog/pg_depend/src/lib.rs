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
    // C sizeof(FormData_pg_depend) == 28; Rust layout may differ.
    let max_slots = referenced
        .len()
        .min(catalog_indexing::MAX_CATALOG_MULTI_INSERT_BYTES / 28);
    let mut indstate = None;
    let mut tuples = std::vec::Vec::with_capacity(max_slots);
    for r in referenced {
        if isObjectPinned(r) {
            continue;
        }
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
        tuples.push(heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?);
        if tuples.len() == max_slots {
            if indstate.is_none() {
                indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &rel)?);
            }
            catalog_indexing::CatalogTuplesMultiInsertWithInfo(
                mcx,
                &rel,
                core::mem::take(&mut tuples),
                indstate.as_mut().unwrap(),
            )?;
        }
    }
    if !tuples.is_empty() {
        if indstate.is_none() {
            indstate = Some(catalog_indexing::CatalogOpenIndexes(mcx, &rel)?);
        }
        catalog_indexing::CatalogTuplesMultiInsertWithInfo(
            mcx,
            &rel,
            tuples,
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
    let kept = eliminate_duplicate_dependencies_slice(referenced);
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

// Narrow find_expr_references_walker lane for single-rel expressions. It
// duplicates arms of catalog_dependency's full walker by constraint:
// catalog_dependency depends on this crate, so delegating would cycle.
struct FindExprRefs<'a, 'mcx> {
    mcx: Mcx<'mcx>,
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
                // OID-alias literals referring to an existing object add a
                // reference to that object (dependency.c Const arm).
                if !c.constisnull {
                    const REGPROC: Oid = 24;
                    const REGPROCEDURE: Oid = 2202;
                    const REGOPER: Oid = 2203;
                    const REGOPERATOR: Oid = 2204;
                    const REGCLASS: Oid = 2205;
                    const REGTYPE: Oid = 2206;
                    const REGCOLLATION: Oid = 4191;
                    const REGCONFIG: Oid = 3734;
                    const REGDICTIONARY: Oid = 3769;
                    const REGNAMESPACE: Oid = 4089;
                    const REGROLE: Oid = 4096;
                    const NAMESPACE_CLASS: Oid = 2615;
                    let objoid = c.constvalue.as_oid();
                    match c.consttype {
                        REGPROC | REGPROCEDURE => {
                            if lsyscache::function::get_func_name(self.mcx, objoid)?.is_some() {
                                addrs.push(ObjectAddress::set(PROC_CLASS, objoid));
                            }
                        }
                        REGOPER | REGOPERATOR => {
                            if lsyscache::operator::get_opname(self.mcx, objoid)?.is_some() {
                                addrs.push(ObjectAddress::set(OPER_CLASS, objoid));
                            }
                        }
                        REGCLASS => {
                            if lsyscache::relation::get_rel_name(self.mcx, objoid)?.is_some() {
                                addrs.push(ObjectAddress::set(RELATION_CLASS, objoid));
                            }
                        }
                        REGTYPE => {
                            if syscache_seams::lookup_pg_type_shape::call(objoid)?.is_some() {
                                addrs.push(ObjectAddress::set(TYPE_CLASS, objoid));
                            }
                        }
                        REGCOLLATION => {
                            if lsyscache::misc::get_collation_name(self.mcx, objoid)?.is_some() {
                                addrs.push(ObjectAddress::set(COLL_CLASS, objoid));
                            }
                        }
                        REGNAMESPACE => {
                            if lsyscache::misc::get_namespace_name(self.mcx, objoid)?.is_some() {
                                addrs.push(ObjectAddress::set(NAMESPACE_CLASS, objoid));
                            }
                        }
                        REGCONFIG | REGDICTIONARY => panic!(
                            "find_expr_references_walker (dependency.c): regconfig/\
                             regdictionary literal; tsearch catalogs unported lane"
                        ),
                        REGROLE => {
                            return Err(Box::new(
                                types_error::PgError::new(
                                    types_error::ERROR,
                                    "constant of the type regrole cannot be used here"
                                        .to_string(),
                                )
                                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                            ));
                        }
                        _ => {}
                    }
                }
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
            T_NullIfExpr => {
                addrs.push(ObjectAddress::set(
                    OPER_CLASS,
                    node.as_null_if_expr().expect("NullIfExpr").opno,
                ));
            }
            T_RowExpr => {
                addrs.push(ObjectAddress::set(
                    TYPE_CLASS,
                    node.as_row_expr().expect("RowExpr").row_typeid,
                ));
            }
            T_RowCompareExpr => {
                let r = node.as_row_compare_expr().expect("RowCompareExpr");
                const OPFAMILY_CLASS: Oid = 2753;
                for opno in &r.opnos {
                    addrs.push(ObjectAddress::set(OPER_CLASS, opno));
                }
                for opfamily in &r.opfamilies {
                    addrs.push(ObjectAddress::set(OPFAMILY_CLASS, opfamily));
                }
            }
            T_CoerceToDomain => {
                addrs.push(ObjectAddress::set(
                    TYPE_CLASS,
                    node.as_coerce_to_domain().expect("CoerceToDomain").resulttype,
                ));
            }
            T_CollateExpr => {
                // C records collOid unconditionally, default included.
                addrs.push(ObjectAddress::set(
                    COLL_CLASS,
                    node.as_collate_expr().expect("CollateExpr").collOid,
                ));
            }
            T_SubscriptingRef => {
                let s = node.as_subscripting_ref().expect("SubscriptingRef");
                if s.refrestype != s.refcontainertype && s.refrestype != s.refelemtype {
                    addrs.push(ObjectAddress::set(TYPE_CLASS, s.refrestype));
                }
            }
            T_FieldSelect => {
                let fselect =
                    node.as_variant::<types_nodes::primnodes::FieldSelect>().expect("FieldSelect");
                let argtype = lsyscache::getBaseType(nodes_core::expr_type(fselect.arg))?;
                let reltype = lsyscache::get_typ_typrelid(argtype)?;
                if reltype != 0 {
                    addrs.push(ObjectAddress::sub_set(
                        RELATION_CLASS,
                        reltype,
                        fselect.fieldnum as i32,
                    ));
                } else {
                    addrs.push(ObjectAddress::set(TYPE_CLASS, fselect.resulttype));
                }
                if fselect.resultcollid != 0 && fselect.resultcollid != DEFAULT_COLLATION_OID {
                    addrs.push(ObjectAddress::set(COLL_CLASS, fselect.resultcollid));
                }
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
            T_ArrayCoerceExpr => {
                let a = node.as_array_coerce_expr().expect("ArrayCoerceExpr");
                addrs.push(ObjectAddress::set(TYPE_CLASS, a.resulttype));
                if a.resultcollid != 0 && a.resultcollid != DEFAULT_COLLATION_OID {
                    addrs.push(ObjectAddress::set(COLL_CLASS, a.resultcollid));
                }
            }
            T_ConvertRowtypeExpr => {
                let c = node.as_convert_rowtype_expr().expect("ConvertRowtypeExpr");
                addrs.push(ObjectAddress::set(TYPE_CLASS, c.resulttype));
            }
            // C has no SQLValueFunction case: expression_tree_walker leaf,
            // built-in pinned result types, no dependency recorded.
            // The SQL/JSON node set likewise has no dependency.c case:
            // contained coercions carry the deps via the default recursion.
            // XmlExpr likewise has no dependency.c case: default recursion.
            // CoerceToDomainValue (a domain CHECK's VALUE) likewise has no
            // dependency.c case: default recursion, no dependency recorded.
            T_BoolExpr | T_NullTest | T_BooleanTest | T_CaseExpr | T_CaseWhen
            | T_CaseTestExpr | T_CoalesceExpr | T_MinMaxExpr | T_ArrayExpr | T_List
            | T_SQLValueFunction | T_XmlExpr | T_JsonExpr | T_JsonValueExpr
            | T_JsonConstructorExpr | T_JsonIsPredicate | T_JsonBehavior
            | T_CoerceToDomainValue => {}
            other => panic!(
                "find_expr_references_walker (dependency.c): {other:?}; unported lane"
            ),
        }
        nodes_core::expression_tree_walker(node, self)
    }
}

// recordDependencyOnSingleRelExpr (dependency.c) over the committed
// expression node set.
pub fn recordDependencyOnSingleRelExpr<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &ObjectAddress,
    expr: types_nodes::Node<'mcx>,
    rel_id: Oid,
    behavior: DependencyType,
    self_behavior: DependencyType,
    reverse_self: bool,
) -> PgResult<()> {
    let mut addrs: mcx::PgVec<'mcx, ObjectAddress> = mcx::PgVec::new_in(mcx);
    nodes_core::NodeWalker::visit(&mut FindExprRefs { mcx, rel_id, addrs: &mut addrs }, expr)?;
    eliminate_duplicate_dependencies(&mut addrs);

    if (behavior != self_behavior || reverse_self) && !addrs.is_empty() {
        let mut self_addrs: mcx::PgVec<'mcx, ObjectAddress> = mcx::PgVec::new_in(mcx);
        let mut rest: mcx::PgVec<'mcx, ObjectAddress> = mcx::PgVec::new_in(mcx);
        for a in addrs.iter() {
            if a.classId == RELATION_CLASS && a.objectId == rel_id {
                self_addrs.push(*a);
            } else {
                rest.push(*a);
            }
        }
        if reverse_self {
            // C dependency.c:1656-1671: the referenced columns become
            // dependent on the whole depender, not the other way around.
            for a in self_addrs.iter() {
                recordDependencyOn(mcx, a, depender, self_behavior)?;
            }
        } else {
            recordMultipleDependencies(mcx, depender, &self_addrs, self_behavior)?;
        }
        return recordMultipleDependencies(mcx, depender, &rest, behavior);
    }
    recordMultipleDependencies(mcx, depender, &addrs, behavior)
}

// eliminate_duplicate_dependencies (dependency.c): sort, drop identicals; a
// whole-object ref (subId 0 sorts first) collapses into the first column ref
// of the same object that follows it.
fn eliminate_duplicate_dependencies_slice(addrs: &mut [ObjectAddress]) -> usize {
    if addrs.len() <= 1 {
        return addrs.len();
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
    kept
}

fn eliminate_duplicate_dependencies(addrs: &mut mcx::PgVec<'_, ObjectAddress>) {
    let kept = eliminate_duplicate_dependencies_slice(addrs);
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

// changeDependenciesOf (pg_depend.c).
pub fn changeDependenciesOf<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    oldObjectId: Oid,
    newObjectId: Oid,
) -> PgResult<i64> {
    let mut count = 0i64;
    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let keys =
        [oid_key(Anum_pg_depend_classid, classId), oid_key(Anum_pg_depend_objid, oldObjectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
    let desc = rel.descr();
    let natts = desc.natts as usize;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replace.resize(natts, false);
        values[Anum_pg_depend_objid - 1] = Datum::from_oid(newObjectId);
        replace[Anum_pg_depend_objid - 1] = true;
        let tid = tup.t_self;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &tid, &mut newtup)?;
        count += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)?;
    Ok(count)
}

// changeDependenciesOn (pg_depend.c).
pub fn changeDependenciesOn<'mcx>(
    mcx: Mcx<'mcx>,
    refClassId: Oid,
    oldRefObjectId: Oid,
    newRefObjectId: Oid,
) -> PgResult<i64> {
    if isObjectPinned(&ObjectAddress::set(refClassId, oldRefObjectId)) {
        return Err(Box::new(
            types_error::PgError::error(format!(
                "cannot remove dependency on object {refClassId}/{oldRefObjectId} because it is a system object"
            ))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let new_is_pinned = isObjectPinned(&ObjectAddress::set(refClassId, newRefObjectId));

    let mut count = 0i64;
    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let keys = [
        oid_key(Anum_pg_depend_refclassid, refClassId),
        oid_key(Anum_pg_depend_refobjid, oldRefObjectId),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependReferenceIndexId, true, None, &keys)?;
    let desc = rel.descr();
    let natts = desc.natts as usize;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
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

// get_index_ref_constraints (pg_depend.c): FK constraints depending on the index.
pub fn get_index_ref_constraints<'mcx>(
    mcx: Mcx<'mcx>,
    index_id: Oid,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    const ConstraintRelationId: Oid = 2606;
    const DEPENDENCY_NORMAL: i8 = b'n' as i8;
    let mut result: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_depend_refclassid, types_core::RELATION_RELATION_ID),
        oid_key(Anum_pg_depend_refobjid, index_id),
        int4_key(Anum_pg_depend_refobjsubid, 0),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependReferenceIndexId, true, None, &keys)?;
    let desc = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let classid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_depend_classid as i32, desc, &mut isnull)
        }
        .as_oid();
        let objid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_depend_objid as i32, desc, &mut isnull)
        }
        .as_oid();
        let objsubid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_depend_objsubid as i32, desc, &mut isnull)
        }
        .as_i32();
        let deptype = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_depend_deptype as i32, desc, &mut isnull)
        }
        .as_i8();
        if classid == ConstraintRelationId && objsubid == 0 && deptype == DEPENDENCY_NORMAL {
            result.push(objid);
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
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
            return Err(Box::new(
                types_error::PgError::new(
                    types_error::ERROR,
                    format!(
                        "{} is already a member of extension \"{}\"",
                        describe_object(mcx, object)?,
                        extension_name_or_lookup_fail(oldext)?
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        return Err(Box::new(
            types_error::PgError::new(
                types_error::ERROR,
                format!(
                    "{} is not a member of extension \"{}\"",
                    describe_object(mcx, object)?,
                    extension_name_or_lookup_fail(CurrentExtensionObject())?
                ),
            )
            .with_detail(
                "An extension is not allowed to replace an object that it does not own.",
            )
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }

    let extension = ObjectAddress::set(types_core::EXTENSION_RELATION_ID, CurrentExtensionObject());
    recordDependencyOn(mcx, object, &extension, DependencyType::Extension)
}

fn describe_object(mcx: Mcx<'_>, object: &ObjectAddress) -> PgResult<String> {
    Ok(objectaddress_seams::get_object_description::call(
        mcx,
        object.classId,
        object.objectId,
        object.objectSubId,
        false,
    )?
    .expect("missing_ok=false"))
}

fn extension_name_or_lookup_fail(ext_oid: Oid) -> PgResult<String> {
    Ok(extension_seams::get_extension_name::call(ext_oid)?
        .unwrap_or_else(|| panic!("cache lookup failed for extension {ext_oid}")))
}

pub fn checkMembershipInCurrentExtension(mcx: Mcx<'_>, object: &ObjectAddress) -> PgResult<()> {
    debug_assert!(object.objectSubId == 0);
    if !creating_extension() {
        return Ok(());
    }
    let oldext = getExtensionOfObject(mcx, object.classId, object.objectId)?;
    if oldext == CurrentExtensionObject() {
        return Ok(());
    }
    Err(Box::new(
        types_error::PgError::new(
            types_error::ERROR,
            format!(
                "{} is not a member of extension \"{}\"",
                describe_object(mcx, object)?,
                extension_name_or_lookup_fail(CurrentExtensionObject())?
            ),
        )
        .with_detail(
            "An extension may only use CREATE ... IF NOT EXISTS to skip object creation \
             if the conflicting object is one that it already owns.",
        )
        .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    ))
}

pub fn deleteDependencyRecordsForSpecific<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    deptype: i8,
    refclassId: Oid,
    refobjectId: Oid,
) -> PgResult<i64> {
    let mut count = 0i64;
    let rel = table::table_open(mcx, DependRelationId, RowExclusiveLock)?;
    let keys = [oid_key(Anum_pg_depend_classid, classId), oid_key(Anum_pg_depend_objid, objectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
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
        if dep_attr(&view, Anum_pg_depend_refclassid, desc).as_oid() == refclassId
            && dep_attr(&view, Anum_pg_depend_refobjid, desc).as_oid() == refobjectId
            && dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8() == deptype
        {
            let tid = view.t_self;
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
            count += 1;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)?;
    Ok(count)
}

pub fn getAutoExtensionsOfObject<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let mut result: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [oid_key(Anum_pg_depend_classid, classId), oid_key(Anum_pg_depend_objid, objectId)];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependDependerIndexId, true, None, &keys)?;
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
                == DependencyType::AutoExtension.as_char()
        {
            result.push(dep_attr(&view, Anum_pg_depend_refobjid, desc).as_oid());
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

pub fn getExtensionType(mcx: Mcx<'_>, extensionOid: Oid, typname: &str) -> PgResult<Oid> {
    let mut result = types_core::InvalidOid;
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_depend_refclassid, types_core::EXTENSION_RELATION_ID),
        oid_key(Anum_pg_depend_refobjid, extensionOid),
        int4_key(Anum_pg_depend_refobjsubid, 0),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependReferenceIndexId, true, None, &keys)?;
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
        if dep_attr(&view, Anum_pg_depend_classid, desc).as_oid() == TYPE_CLASS
            && dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8()
                == DependencyType::Extension.as_char()
        {
            let typoid = dep_attr(&view, Anum_pg_depend_objid, desc).as_oid();
            let Some((name, _nsp)) = syscache_seams::pg_type_name_namespace::call(typoid)? else {
                continue;
            };
            if name.name_str() == typname.as_bytes() {
                result = typoid;
                break;
            }
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

pub fn recordDependencyOnOwner<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    owner: Oid,
) -> PgResult<()> {
    pg_shdepend::recordDependencyOnOwner(mcx, classId, objectId, owner)
}

pub fn recordDependencyOnTablespace<'mcx>(
    mcx: Mcx<'mcx>,
    classId: Oid,
    objectId: Oid,
    tablespace: Oid,
) -> PgResult<()> {
    pg_shdepend::recordDependencyOnTablespace(mcx, classId, objectId, tablespace)
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

const RELKIND_SEQUENCE: i8 = b'S' as i8;

fn getOwnedSequences_internal<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i32,
    deptype: Option<DependencyType>,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let mut result: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys3;
    let keys2;
    let keys: &[types_scan::scankey::ScanKeyData] = if attnum != 0 {
        keys3 = [
            oid_key(Anum_pg_depend_refclassid, types_core::RELATION_RELATION_ID),
            oid_key(Anum_pg_depend_refobjid, relid),
            int4_key(Anum_pg_depend_refobjsubid, attnum),
        ];
        &keys3
    } else {
        keys2 = [
            oid_key(Anum_pg_depend_refclassid, types_core::RELATION_RELATION_ID),
            oid_key(Anum_pg_depend_refobjid, relid),
        ];
        &keys2
    };
    let mut scan = genam::systable_beginscan(mcx, &rel, DependReferenceIndexId, true, None, keys)?;
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
        let dep_deptype = dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8();
        let objid = dep_attr(&view, Anum_pg_depend_objid, desc).as_oid();
        if dep_attr(&view, Anum_pg_depend_classid, desc).as_oid()
            == types_core::RELATION_RELATION_ID
            && dep_attr(&view, Anum_pg_depend_objsubid, desc).as_i32() == 0
            && dep_attr(&view, Anum_pg_depend_refobjsubid, desc).as_i32() != 0
            && (dep_deptype == DependencyType::Auto.as_char()
                || dep_deptype == DependencyType::Internal.as_char())
            && lsyscache::relation::get_rel_relkind(objid)? == RELKIND_SEQUENCE
        {
            if deptype.is_none_or(|d| d.as_char() == dep_deptype) {
                result.push(objid);
            }
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

// pg_get_serial_sequence's dependency scan (ruleutils.c:2861): objids of
// AUTO/INTERNAL pg_depend entries from whole pg_class objects on
// (relid, attnum), in DependReferenceIndexId scan order. The
// relkind == SEQUENCE filter stays with the caller, as in C.
pub fn get_serial_sequence_candidates<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i32,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_depend_refclassid, types_core::RELATION_RELATION_ID),
        oid_key(Anum_pg_depend_refobjid, relid),
        int4_key(Anum_pg_depend_refobjsubid, attnum),
    ];
    let mut scan = genam::systable_beginscan(mcx, &rel, DependReferenceIndexId, true, None, &keys)?;
    let mut out: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
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
        let deptype = dep_attr(&view, Anum_pg_depend_deptype, desc).as_i8();
        if dep_attr(&view, Anum_pg_depend_classid, desc).as_oid()
            == types_core::RELATION_RELATION_ID
            && dep_attr(&view, Anum_pg_depend_objsubid, desc).as_i32() == 0
            && (deptype == DependencyType::Auto.as_char()
                || deptype == DependencyType::Internal.as_char())
        {
            out.push(dep_attr(&view, Anum_pg_depend_objid, desc).as_oid());
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(out)
}

pub fn getOwnedSequences<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    getOwnedSequences_internal(mcx, relid, 0, None)
}

// getIdentitySequence (pg_depend.c).
pub fn getIdentitySequence<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i32,
    missing_ok: bool,
) -> PgResult<Oid> {
    let mut relid = relid;
    let mut attnum = attnum;
    // The identity sequence hangs off the topmost partitioned table, which
    // might have a different column order than the partition.
    if lsyscache::relation::get_rel_relispartition(relid)? {
        let ancestors = pg_inherits::get_partition_ancestors(mcx, relid)?;
        let attname = lsyscache::attribute::get_attname(mcx, relid, attnum as i16, false)?
            .expect("get_attname !missing_ok returns Some");
        relid = *ancestors.last().expect("partition has ancestors");
        attnum = lsyscache::attribute::get_attnum(relid, attname.as_str())? as i32;
        if attnum == 0 {
            panic!(
                "cache lookup failed for attribute \"{}\" of relation {relid}",
                attname.as_str()
            );
        }
    }
    let seqlist = getOwnedSequences_internal(mcx, relid, attnum, Some(DependencyType::Internal))?;
    if seqlist.len() > 1 {
        panic!("more than one owned sequence found for column {relid}.{attnum}");
    }
    let Some(&seq) = seqlist.first() else {
        if missing_ok {
            return Ok(types_core::InvalidOid);
        }
        panic!("no owned sequence found for identity column {relid}.{attnum}");
    };
    Ok(seq)
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
