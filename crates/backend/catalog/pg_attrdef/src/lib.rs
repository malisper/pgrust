//! pg_attrdef.c, StoreAttrDefault lane. Dependency recording
//! (recordDependencyOn/recordDependencyOnSingleRelExpr) is unported: DROP of a
//! defaulted column/table leaves the pg_attrdef row behind (pg_depend unit).

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

    Ok(attrdef_oid)
}

// Aligned with C's ATTNUM cache-lookup elog.
#[cold]
#[inline(never)]
fn attr_lookup_failed(attnum: AttrNumber, relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for attribute {attnum} of relation {relid}"
    )))
}
