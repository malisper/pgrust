//! pg_attrdef.c, StoreAttrDefault/RemoveAttrDefaultById lane.
//! recordDependencyOnSingleRelExpr is sliced: defaults whose expressions
//! reference non-pinned objects are loud.

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::fmgr::{F_INT2EQ, F_OIDEQ};
use types_core::{
    AttrNumber, Oid, RegProcedure, ATTRIBUTE_RELATION_ID, ATTR_DEFAULT_OID_INDEX_ID,
    ATTR_DEFAULT_RELATION_ID,
};
use types_error::{PgError, PgResult};
use types_nodes::Node;
use types_rel::{Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub const Anum_pg_attrdef_oid: AttrNumber = 1;
pub const Anum_pg_attrdef_adrelid: AttrNumber = 2;
pub const Anum_pg_attrdef_adnum: AttrNumber = 3;
pub const Anum_pg_attrdef_adbin: AttrNumber = 4;

const Anum_pg_attribute_attrelid: AttrNumber = 1;
const Anum_pg_attribute_attnum: AttrNumber = 5;
const Anum_pg_attribute_atthasdef: AttrNumber = 13;
const AttributeRelidNumIndexId: Oid = 2659;

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

pub fn StoreAttrDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    expr: Node<'mcx>,
) -> PgResult<Oid> {
    let adbin = outfuncs::nodeToString(mcx, expr)?;
    let adrel = table::table_open(mcx, ATTR_DEFAULT_RELATION_ID, RowExclusiveLock)?;

    let attrdef_oid = catalog::GetNewOidWithIndex(
        mcx,
        &adrel,
        ATTR_DEFAULT_OID_INDEX_ID,
        Anum_pg_attrdef_oid,
    )?;
    let adbin_text = varlena::cstring_to_text(mcx, adbin.as_bytes())?;
    let values = [
        Datum::from_oid(attrdef_oid),
        Datum::from_oid(rel.rd_id),
        Datum::from_i16(attnum),
        Datum::from_usize(adbin_text.as_bytes().as_ptr() as usize),
    ];
    let nulls = [false; 4];
    let mut tuple = heaptuple::heap_form_tuple(mcx, adrel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &adrel, &mut tuple)?;
    adrel.close(RowExclusiveLock)?;

    // Flip pg_attribute.atthasdef on the column's live row.
    let attrrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let keys = [
        eq_key(Anum_pg_attribute_attrelid, F_OIDEQ, Datum::from_oid(rel.rd_id)),
        eq_key(Anum_pg_attribute_attnum, F_INT2EQ, Datum::from_i16(attnum)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &attrrel, AttributeRelidNumIndexId, true, None, &keys)?;
    let atttup = match genam::systable_getnext(mcx, &mut scan)? {
        Some(t) => t,
        None => return Err(attr_lookup_failed(attnum, rel.rd_id)),
    };
    let natts = attrrel.descr().natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[(Anum_pg_attribute_atthasdef - 1) as usize] = Datum::from_bool(true);
    repl[(Anum_pg_attribute_atthasdef - 1) as usize] = true;
    let mut newtup = heaptuple::heap_modify_tuple(
        mcx,
        atttup,
        attrrel.descr(),
        &repl_values,
        &repl_isnull,
        &repl,
    )?;
    let otid = atttup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;
    attrrel.close(RowExclusiveLock)?;

    // C: generated column defaults depend INTERNAL, plain defaults AUTO.
    let deptype = if rel.rd_att.attr(attnum as usize - 1).attgenerated != 0 {
        pg_depend::DependencyType::Internal
    } else {
        pg_depend::DependencyType::Auto
    };
    let defobject = pg_depend::ObjectAddress::set(ATTR_DEFAULT_RELATION_ID, attrdef_oid);
    let colobject =
        pg_depend::ObjectAddress::sub_set(types_core::RELATION_RELATION_ID, rel.rd_id, attnum as i32);
    pg_depend::recordDependencyOn(mcx, &defobject, &colobject, deptype)?;
    record_single_rel_expr_deps(mcx, &defobject, rel.rd_id, expr)?;

    Ok(attrdef_oid)
}

// recordDependencyOnSingleRelExpr slice (behavior == self_behavior == NORMAL
// per StoreAttrDefault): pinned references record nothing; same-relation Vars
// record NORMAL column deps; anything else is loud.
fn record_single_rel_expr_deps<'mcx>(
    mcx: Mcx<'mcx>,
    depender: &pg_depend::ObjectAddress,
    relid: Oid,
    expr: Node<'mcx>,
) -> PgResult<()> {
    const MAX_HEAP_ATTRIBUTE_NUMBER: usize = 1600;
    struct W {
        attnums: [bool; MAX_HEAP_ATTRIBUTE_NUMBER + 1],
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            use types_nodes::NodeTag::*;
            const TYPE_CLASS: Oid = types_core::TYPE_RELATION_ID;
            const PROC_CLASS: Oid = 1255;
            const OPER_CLASS: Oid = 2617;
            const COLL_CLASS: Oid = 3456;
            let pinned = |class: Oid, oid: Oid| oid == 0 || catalog::IsPinnedObject(class, oid);
            let ok = match node.node_tag() {
                T_Var => {
                    let v = node.as_var().expect("Var");
                    if v.varlevelsup != 0 || v.varno != 1 || v.varattno < 0 {
                        panic!("unported: recordDependencyOnSingleRelExpr non-self Var");
                    }
                    self.attnums[v.varattno as usize] = true;
                    true
                }
                T_Const => {
                    let c = node.as_const().expect("Const");
                    pinned(TYPE_CLASS, c.consttype) && pinned(COLL_CLASS, c.constcollid)
                }
                T_FuncExpr => {
                    let f = node.as_func_expr().expect("FuncExpr");
                    pinned(PROC_CLASS, f.funcid) && pinned(TYPE_CLASS, f.funcresulttype)
                }
                T_OpExpr => {
                    let o = node.as_op_expr().expect("OpExpr");
                    pinned(OPER_CLASS, o.opno) && pinned(TYPE_CLASS, o.opresulttype)
                }
                T_RelabelType | T_CoerceViaIO | T_BoolExpr | T_CaseExpr | T_CaseWhen
                | T_NullTest | T_CoalesceExpr | T_MinMaxExpr | T_List => true,
                other => panic!(
                    "unported: recordDependencyOnSingleRelExpr over {other:?} default expression"
                ),
            };
            if !ok {
                panic!(
                    "unported: recordDependencyOnSingleRelExpr non-pinned reference in \
                     default expression"
                );
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { attnums: [false; MAX_HEAP_ATTRIBUTE_NUMBER + 1] };
    nodes_core::NodeWalker::visit(&mut w, expr)?;
    for (attnum, seen) in w.attnums.iter().enumerate() {
        if *seen {
            let refobj = pg_depend::ObjectAddress::sub_set(
                types_core::RELATION_RELATION_ID,
                relid,
                attnum as i32,
            );
            pg_depend::recordDependencyOn(
                mcx,
                depender,
                &refobj,
                pg_depend::DependencyType::Normal,
            )?;
        }
    }
    Ok(())
}

// GetAttrDefaultOid (pg_attrdef.c): pg_attrdef row for (adrelid, adnum).
pub fn GetAttrDefaultOid<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: AttrNumber) -> PgResult<Oid> {
    const AttrDefaultIndexId: Oid = 2656;
    let adrel = table::table_open(mcx, ATTR_DEFAULT_RELATION_ID, types_rel::AccessShareLock)?;
    let keys = [
        eq_key(Anum_pg_attrdef_adrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_attrdef_adnum, F_INT2EQ, Datum::from_i16(attnum)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &adrel, AttrDefaultIndexId, true, None, &keys)?;
    let mut result = types_core::InvalidOid;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_attrdef oid column under its descriptor.
        result = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_attrdef_oid as i32, adrel.descr(), &mut isnull)
        }
        .as_oid();
    }
    genam::systable_endscan(mcx, scan)?;
    adrel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

// GetAttrDefaultColumnAddress (pg_attrdef.c); InvalidOid objectId = no entry.
pub fn GetAttrDefaultColumnAddress<'mcx>(
    mcx: Mcx<'mcx>,
    attrdefoid: Oid,
) -> PgResult<pg_depend::ObjectAddress> {
    let adrel = table::table_open(mcx, ATTR_DEFAULT_RELATION_ID, types_rel::AccessShareLock)?;
    let keys = [eq_key(Anum_pg_attrdef_oid, F_OIDEQ, Datum::from_oid(attrdefoid))];
    let mut scan =
        genam::systable_beginscan(mcx, &adrel, ATTR_DEFAULT_OID_INDEX_ID, true, None, &keys)?;
    let mut result =
        pg_depend::ObjectAddress::set(types_core::InvalidOid, types_core::InvalidOid);
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let desc = adrel.descr();
        let get = |anum: i32| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_attrdef columns under its descriptor.
            let d = unsafe { types_tuple::heap_getattr(tup, anum, desc, &mut isnull) };
            debug_assert!(!isnull);
            d
        };
        result = pg_depend::ObjectAddress::sub_set(
            types_core::RELATION_RELATION_ID,
            get(Anum_pg_attrdef_adrelid as i32).as_oid(),
            get(Anum_pg_attrdef_adnum as i32).as_i16() as i32,
        );
    }
    genam::systable_endscan(mcx, scan)?;
    adrel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

pub fn RemoveAttrDefaultById<'mcx>(mcx: Mcx<'mcx>, attrdef_id: Oid) -> PgResult<()> {
    let adrel = table::table_open(mcx, ATTR_DEFAULT_RELATION_ID, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_attrdef_oid, F_OIDEQ, Datum::from_oid(attrdef_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &adrel, ATTR_DEFAULT_OID_INDEX_ID, true, None, &keys)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for attrdef {attrdef_id}"));
    let desc = adrel.descr();
    let get = |anum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_attrdef columns under its descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, anum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };
    let myrelid = get(Anum_pg_attrdef_adrelid as i32).as_oid();
    let myattnum = get(Anum_pg_attrdef_adnum as i32).as_i16();

    let myrel = table::table_open(mcx, myrelid, types_rel::AccessExclusiveLock)?;

    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&adrel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    adrel.close(RowExclusiveLock)?;

    let attrrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let keys = [
        eq_key(Anum_pg_attribute_attrelid, F_OIDEQ, Datum::from_oid(myrelid)),
        eq_key(Anum_pg_attribute_attnum, F_INT2EQ, Datum::from_i16(myattnum)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &attrrel, AttributeRelidNumIndexId, true, None, &keys)?;
    let atttup = match genam::systable_getnext(mcx, &mut scan)? {
        Some(t) => t,
        None => return Err(attr_lookup_failed(myattnum, myrelid)),
    };
    let natts = attrrel.descr().natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[(Anum_pg_attribute_atthasdef - 1) as usize] = Datum::from_bool(false);
    repl[(Anum_pg_attribute_atthasdef - 1) as usize] = true;
    let mut newtup = heaptuple::heap_modify_tuple(
        mcx,
        atttup,
        attrrel.descr(),
        &repl_values,
        &repl_isnull,
        &repl,
    )?;
    let otid = atttup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;
    attrrel.close(RowExclusiveLock)?;
    myrel.close(types_rel::NoLock)
}

// Aligned with C's ATTNUM cache-lookup elog.
#[cold]
#[inline(never)]
fn attr_lookup_failed(attnum: AttrNumber, relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for attribute {attnum} of relation {relid}"
    )))
}

