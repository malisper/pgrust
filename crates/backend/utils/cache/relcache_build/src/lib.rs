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

use datum::{Datum, VarlenaRef};
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

// Shared bounded reader for every fixed-width NameData catalog column.
//
// A NameData column is NAMEDATALEN wide, and C reads it as a fixed field via
// GETSTRUCT + NameStr; C is safe only because that field lies within the heap
// tuple's validated t_len extent. A Datum carries no length of its own, so a
// truncated/crafted catalog tuple would drive a blind NAMEDATALEN read past the
// tuple image (CWE-125 out-of-bounds read / heap disclosure). Mirror C's
// implicit bound explicitly: clamp the copy to the bytes remaining between the
// datum pointer and the end of the containing tuple image
// (header_ptr()..header_ptr()+t_len), then NUL-pad to NAMEDATALEN. A valid name
// has its full NAMEDATALEN bytes in range and is returned unchanged; a short
// datum yields a NUL-padded prefix instead of an OOB read.
fn name_from(tup: &HeapTupleData<'_>, d: Datum) -> NameData {
    let mut n = NameData::default();
    let dp = d.as_usize() as *const u8;
    let end = tup.header_ptr() as usize + tup.t_len as usize;
    let take = end.saturating_sub(dp as usize).min(NAMEDATALEN as usize);
    // SAFETY: `take` bytes lie within the tuple image [header_ptr, +t_len),
    // whose readability for t_len bytes is the HeapTupleData construction
    // invariant; the remaining tail of `n.data` stays NUL from Default.
    let bytes = unsafe { slice::from_raw_parts(dp, take) };
    n.data[..take].copy_from_slice(bytes);
    n
}

// Elements of an int2vector/oidvector image that actually fit within the
// datum's varlena length word. The on-image `dim1` element count is read
// verbatim from catalog bytes; C trusts it because int2vectorin/oidvectorin
// validated it at DDL time, but a crafted pg_index tuple can plant an inflated
// or negative `dim1` that would drive the values slice far past the tuple image
// (CWE-125 out-of-bounds read / heap disclosure). Clamp the count to what the
// varlena length word (the sanctioned per-column extent, established when the
// tuple was deformed) can hold: a valid vector's `dim1` fits exactly and is
// returned unchanged, matching C's RelationInitIndexAccessInfo; a forged
// `dim1` yields a short slice that the caller's indnkeyatts/indnatts
// consistency check rejects as a clean error instead of reading out of bounds.
fn vector_fit_len(varsize: usize, dim1: i32, elemsz: usize) -> usize {
    let fit = varsize.saturating_sub(array::VECTOR_HDRSZ) / elemsz;
    if dim1 < 0 {
        0
    } else {
        (dim1 as usize).min(fit)
    }
}

// SAFETY contract for both: d comes off a not-null oidvector/int2vector
// column; typstorage is plain so the image is never packed or toasted, and
// the values tail follows the 24-byte header in place. The values slice is
// bounded by vector_fit_len against the datum's varlena length word so a
// crafted on-image `dim1` can never extend the read past the tuple.
unsafe fn oidvector_values<'a>(d: Datum) -> &'a [Oid] {
    let p = d.as_usize() as *const array::oidvector;
    let varsize = VarlenaRef::from_ptr(d.as_usize() as *const u8).varsize();
    let n = vector_fit_len(varsize, (*p).dim1, core::mem::size_of::<Oid>());
    slice::from_raw_parts(p.add(1) as *const Oid, n)
}

unsafe fn int2vector_values<'a>(d: Datum) -> &'a [i16] {
    let p = d.as_usize() as *const array::int2vector;
    let varsize = VarlenaRef::from_ptr(d.as_usize() as *const u8).varsize();
    let n = vector_fit_len(varsize, (*p).dim1, core::mem::size_of::<i16>());
    slice::from_raw_parts(p.add(1) as *const i16, n)
}
