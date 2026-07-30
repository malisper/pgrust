#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod attrs;
mod domain;
mod index;
mod triggers;
mod pg_class;
mod policies;
mod rules;
#[cfg(test)]
mod tests;

use core::slice;

use datum::Datum;
use types_core::catalog::C_COLLATION_OID;
use types_core::fmgr::{F_OIDEQ, NAMEDATALEN};
use types_core::primitive::RegProcedure;
use types_core::{AttrNumber, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData, StrategyNumber};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

pub fn init_seams() {
    relcache_build_seams::scan_pg_relation::set(pg_class::scan_pg_relation);
    relcache_build_seams::scan_pg_rewrite::set(rules::scan_pg_rewrite);
    relcache_build_seams::scan_pg_policy::set(policies::scan_pg_policy);
    relcache_build_seams::relation_build_tuple_desc::set(attrs::relation_build_tuple_desc);
    relcache_build_seams::relation_init_index_access_info::set(
        index::relation_init_index_access_info,
    );
    relcache_build_seams::scan_pg_index_shapes::set(index::scan_pg_index_shapes);
    relcache_build_seams::scan_exclusion_ops::set(index::scan_exclusion_ops);
    relcache_build_seams::build_trigger_desc::set(triggers::build_trigger_desc);
    typcache_seams::scan_domain_check_constraints::set(domain::scan_domain_check_constraints);
    relcache_build_seams::scan_pg_statistic_ext_oids::set(index::scan_pg_statistic_ext_oids);
    relcache_build_seams::scan_pg_constraint_fkeys::set(attrs::scan_pg_constraint_fkeys);
}

pub(crate) fn scan_key(attno: i32, strategy: StrategyNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = strategy;
    key.sk_collation = C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

pub(crate) fn oid_key(attno: i32, oid: Oid) -> ScanKeyData {
    scan_key(attno, BTEqualStrategyNumber, F_OIDEQ, Datum::from_oid(oid))
}

pub(crate) fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a catalog row read under its relation's descriptor;
    // attno is a declared column of that catalog.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

pub(crate) fn req(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> PgResult<Datum> {
    let (d, isnull) = getattr(td, tup, attno);
    if isnull {
        return Err(unexpected_null(attno));
    }
    Ok(d)
}

#[track_caller]
#[cold]
#[inline(never)]
fn unexpected_null(attno: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!("unexpected null in catalog column {attno}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn name_from(d: Datum) -> NameData {
    let mut n = NameData::default();
    // SAFETY: d comes off a NameData column: NAMEDATALEN readable bytes.
    let bytes =
        unsafe { slice::from_raw_parts(d.as_usize() as *const u8, NAMEDATALEN as usize) };
    n.data.copy_from_slice(bytes);
    n
}

// SAFETY contract for both: d comes off a not-null oidvector/int2vector
// column; typstorage is plain so the image is never packed or toasted, and
// the values tail follows the 24-byte header in place.
unsafe fn oidvector_values<'a>(d: Datum) -> &'a [Oid] {
    let p = d.as_usize() as *const array::oidvector;
    slice::from_raw_parts(p.add(1) as *const Oid, (*p).dim1 as usize)
}

unsafe fn int2vector_values<'a>(d: Datum) -> &'a [i16] {
    let p = d.as_usize() as *const array::int2vector;
    slice::from_raw_parts(p.add(1) as *const i16, (*p).dim1 as usize)
}
