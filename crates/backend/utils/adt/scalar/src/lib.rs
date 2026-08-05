//! oid.c comparison slice + the xid.c surface (xid/xid8/cid I/O, cmp, hash,
//! xid_age, mxid_age); the rest of the adt-scalar batch stays todo.

pub mod builtins;
mod currtid;
pub mod datum_ops;
#[cfg(test)]
mod tests;

pub use currtid::currtid_byrelname;
pub use datum_ops::{
    datum_copy, datum_estimate_space, datum_get_size, datum_restore, datum_serialize,
};

use ::types_core::{Oid, TransactionIdIsNormal};
use ::types_error::PgResult;

macro_rules! oid_cmp_ops {
    ($($name:ident: $op:tt;)*) => {$(
        #[inline]
        pub fn $name(arg1: Oid, arg2: Oid) -> bool {
            arg1 $op arg2
        }
    )*};
}

oid_cmp_ops! {
    oideq: ==; oidne: !=;
    oidlt: <;  oidle: <=;
    oidgt: >;  oidge: >=;
}

/// `oidlarger` (oid.c): the greater of two OIDs.
#[inline]
pub fn oidlarger(arg1: Oid, arg2: Oid) -> Oid {
    if arg1 > arg2 { arg1 } else { arg2 }
}

/// `oidsmaller` (oid.c): the lesser of two OIDs.
#[inline]
pub fn oidsmaller(arg1: Oid, arg2: Oid) -> Oid {
    if arg1 < arg2 { arg1 } else { arg2 }
}

/// `xidout` (xid.c) into a caller buffer; returns the byte length.
#[inline]
pub fn xidout(xid: u32, buf: &mut [u8]) -> usize {
    let mut tmp = [0u8; 10];
    let mut n = 0;
    let mut v = xid;
    loop {
        tmp[n] = b'0' + (v % 10) as u8;
        v /= 10;
        n += 1;
        if v == 0 {
            break;
        }
    }
    for i in 0..n {
        buf[i] = tmp[n - 1 - i];
    }
    n
}

#[inline]
pub fn xideq(x1: u32, x2: u32) -> bool {
    x1 == x2
}

#[inline]
pub fn xidneq(x1: u32, x2: u32) -> bool {
    x1 != x2
}

/// `xid_age` (xid.c): age of `xid` relative to the latest stable XID.
/// Permanent XIDs are infinitely old.
pub fn xid_age(xid: u32) -> PgResult<i32> {
    let now = xact_seams::get_stable_latest_transaction_id::call()?;
    if !TransactionIdIsNormal(xid) {
        return Ok(i32::MAX);
    }
    Ok(now.wrapping_sub(xid) as i32)
}

/// `mxid_age` (xid.c): age of `xid` relative to the next multixact ID.
pub fn mxid_age(xid: u32) -> PgResult<i32> {
    let now = ::multixact::ReadNextMultiXactId()?;
    if !::multixact::MultiXactIdIsValid(xid) {
        return Ok(i32::MAX);
    }
    Ok(now.wrapping_sub(xid) as i32)
}

/// xid8 carries a u64 with plain ordering (FullTransactionIdPrecedes).
pub fn xid8cmp(a: u64, b: u64) -> i32 {
    match a.cmp(&b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// tid.c pure slice: (block, offset) with C's on-tuple 3x u16 layout handled
/// at the fmgr boundary.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Tid {
    pub block: u32,
    pub offset: u16,
}

// C strtoul base-10: leading isspace + optional sign, wrapping negation,
// saturate+error past u64.
fn strtoul_c(s: &[u8]) -> Option<(u64, usize)> {
    let mut i = 0;
    while i < s.len() && (s[i] == b' ' || (0x09..=0x0d).contains(&s[i])) {
        i += 1;
    }
    let neg = if i < s.len() && (s[i] == b'-' || s[i] == b'+') {
        i += 1;
        s[i - 1] == b'-'
    } else {
        false
    };
    let start = i;
    let mut v: u64 = 0;
    let mut overflow = false;
    while i < s.len() && s[i].is_ascii_digit() {
        let (nv, o1) = v.overflowing_mul(10);
        let (nv, o2) = nv.overflowing_add((s[i] - b'0') as u64);
        overflow |= o1 | o2;
        v = nv;
        i += 1;
    }
    if i == start || overflow {
        return None;
    }
    Some((if neg { v.wrapping_neg() } else { v }, i))
}

pub fn tidin(s: &[u8]) -> Option<Tid> {
    const NTIDARGS: usize = 2;
    let mut coord = [0usize; NTIDARGS];
    let mut n = 0;
    for (p, &c) in s.iter().enumerate() {
        if n >= NTIDARGS || c == b')' {
            break;
        }
        if c == b',' || (c == b'(' && n == 0) {
            coord[n] = p + 1;
            n += 1;
        }
    }
    if n < NTIDARGS {
        return None;
    }
    let (cvt, used) = strtoul_c(&s[coord[0]..])?;
    if s.get(coord[0] + used) != Some(&b',') {
        return None;
    }
    let block = cvt as u32;
    // C's SIZEOF_LONG>4 arm: accept exactly u32-truncatable or
    // i32-sign-extended values.
    if cvt != block as u64 && cvt != (block as i32) as i64 as u64 {
        return None;
    }
    let (cvt, used) = strtoul_c(&s[coord[1]..])?;
    if s.get(coord[1] + used) != Some(&b')') || cvt > u16::MAX as u64 {
        return None;
    }
    Some(Tid { block, offset: cvt as u16 })
}

pub fn tidout(tid: Tid, buf: &mut [u8]) -> usize {
    buf[0] = b'(';
    let mut n = 1 + ::numutils::pg_ultoa_n(tid.block, &mut buf[1..]);
    buf[n] = b',';
    n += 1;
    n += ::numutils::pg_ultoa_n(tid.offset as u32, &mut buf[n..]);
    buf[n] = b')';
    n + 1
}

pub fn tid_cmp(a: Tid, b: Tid) -> i32 {
    match (a.block, a.offset).cmp(&(b.block, b.offset)) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

// Pure selection cores (C tid.c tidlarger/tidsmaller), factored out of the
// fc wrappers so the value logic is provable without the byref-result
// allocation (proofs/scalar-misc family).
pub fn tid_larger(a: Tid, b: Tid) -> Tid {
    if tid_cmp(a, b) >= 0 {
        a
    } else {
        b
    }
}

pub fn tid_smaller(a: Tid, b: Tid) -> Tid {
    if tid_cmp(a, b) <= 0 {
        a
    } else {
        b
    }
}
