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
    pg_depend::recordDependencyOnSingleRelExpr(
        mcx,
        &defobject,
        expr,
        rel.rd_id,
        pg_depend::DependencyType::Normal,
        pg_depend::DependencyType::Normal,
        false,
    )?;

    Ok(attrdef_oid)
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

// GetAttrDefaultColumnAddress (pg_attrdef.c): (adrelid, adnum) as an
// InvalidOid-signalling pair when the pg_attrdef row is gone.
pub fn GetAttrDefaultColumnAddress<'mcx>(
    mcx: Mcx<'mcx>,
    attrdef_id: Oid,
) -> PgResult<(Oid, AttrNumber)> {
    let adrel = table::table_open(mcx, ATTR_DEFAULT_RELATION_ID, types_rel::AccessShareLock)?;
    let keys = [eq_key(Anum_pg_attrdef_oid, F_OIDEQ, Datum::from_oid(attrdef_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &adrel, ATTR_DEFAULT_OID_INDEX_ID, true, None, &keys)?;
    let mut result = (types_core::InvalidOid, 0 as AttrNumber);
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let desc = adrel.descr();
        let get = |anum: i32| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_attrdef columns under its descriptor.
            let d = unsafe { types_tuple::heap_getattr(tup, anum, desc, &mut isnull) };
            debug_assert!(!isnull);
            d
        };
        result = (
            get(Anum_pg_attrdef_adrelid as i32).as_oid(),
            get(Anum_pg_attrdef_adnum as i32).as_i16(),
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
#[track_caller]
#[cold]
#[inline(never)]
fn attr_lookup_failed(attnum: AttrNumber, relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for attribute {attnum} of relation {relid}"
    )))
}

// pg_attrdef adbin for (adrelid, adnum), as the stored node string.
pub fn GetAttrDefaultBin<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
) -> PgResult<Option<String>> {
    const AttrDefaultIndexId: Oid = 2656;
    let adrel = table::table_open(mcx, ATTR_DEFAULT_RELATION_ID, types_rel::AccessShareLock)?;
    let keys = [
        eq_key(Anum_pg_attrdef_adrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_attrdef_adnum, F_INT2EQ, Datum::from_i16(attnum)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &adrel, AttrDefaultIndexId, true, None, &keys)?;
    let mut result: Option<String> = None;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: adbin under pg_attrdef's descriptor; NOT NULL by catalog contract.
        let d = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_attrdef_adbin as i32, adrel.descr(), &mut isnull)
        };
        assert!(!isnull, "null adbin for relation {relid} attnum {attnum}");
        let p = d.as_usize() as *const u8;
        // SAFETY: live varlena text image through its extent.
        let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let payload = varlena::open_image(mcx, image)?;
        result =
            Some(core::str::from_utf8(payload.as_bytes()).expect("adbin UTF-8").to_string());
    }
    genam::systable_endscan(mcx, scan)?;
    adrel.close(types_rel::AccessShareLock)?;
    Ok(result)
}
