//! pg_lsn.c: LSN I/O, comparison, hash, min/max, and the numeric-bridged
//! arithmetic (mi/pli/mii, numeric_pg_lsn).

#![allow(non_snake_case)]

use adt_numeric::{Num, NumericImage, NumericVar};
use datum::Bytea;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
};

pub mod builtins;
#[cfg(test)]
mod tests;

pub type XLogRecPtr = u64;
pub const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

pub const MAXPG_LSNLEN: usize = 17;
const MAXPG_LSNCOMPONENT: usize = 8;

pub fn pg_lsn_in_internal(s: &[u8]) -> Option<XLogRecPtr> {
    let len1 = s.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if len1 < 1 || len1 > MAXPG_LSNCOMPONENT || s.get(len1) != Some(&b'/') {
        return None;
    }
    let rest = &s[len1 + 1..];
    let len2 = rest.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if len2 < 1 || len2 > MAXPG_LSNCOMPONENT || len2 != rest.len() {
        return None;
    }
    let id = u32::from_str_radix(core::str::from_utf8(&s[..len1]).ok()?, 16).ok()?;
    let off = u32::from_str_radix(core::str::from_utf8(&rest[..len2]).ok()?, 16).ok()?;
    Some(((id as u64) << 32) | off as u64)
}

#[cold]
fn invalid_lsn(s: &str) -> PgError {
    PgError::error(format!("invalid input syntax for type {}: \"{s}\"", "pg_lsn"))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

pub fn pg_lsn_in(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<XLogRecPtr> {
    match pg_lsn_in_internal(s.as_bytes()) {
        Some(lsn) => Ok(lsn),
        None => ereturn(escontext, INVALID_XLOG_REC_PTR, invalid_lsn(s)),
    }
}

pub fn pg_lsn_out_into(lsn: XLogRecPtr, buf: &mut [u8; MAXPG_LSNLEN + 1]) -> usize {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    let mut n = 0;
    for (i, part) in [hi, lo].into_iter().enumerate() {
        if i == 1 {
            buf[n] = b'/';
            n += 1;
        }
        let mut tmp = [0u8; 8];
        let mut v = part;
        let mut k = 8;
        loop {
            k -= 1;
            let d = (v & 0xf) as u8;
            tmp[k] = if d < 10 { b'0' + d } else { b'A' + d - 10 };
            v >>= 4;
            if v == 0 {
                break;
            }
        }
        let len = 8 - k;
        buf[n..n + len].copy_from_slice(&tmp[k..]);
        n += len;
    }
    n
}

pub fn pg_lsn_recv(buf: &mut StringInfo<'_>) -> PgResult<XLogRecPtr> {
    Ok(pqformat::pq_getmsgint64(buf)? as u64)
}

pub fn pg_lsn_send<'mcx>(mcx: Mcx<'mcx>, lsn: XLogRecPtr) -> PgResult<Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint64(&mut b, lsn)?;
    Ok(pqformat::pq_endtypsend(b))
}

pub fn pg_lsn_cmp_internal(a: XLogRecPtr, b: XLogRecPtr) -> i32 {
    match a.cmp(&b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

fn u64_to_numeric(v: u64) -> PgResult<NumericImage> {
    let mut buf = [0u8; 20];
    let mut n = buf.len();
    let mut x = v;
    loop {
        n -= 1;
        buf[n] = b'0' + (x % 10) as u8;
        x /= 10;
        if x == 0 {
            break;
        }
    }
    // SAFETY-free: ASCII digits only.
    let s = core::str::from_utf8(&buf[n..]).expect("ascii digits");
    Ok(adt_numeric::numeric_in(s, -1, None)?.expect("hard-error path returns Err"))
}

/// C `pg_lsn_mi`: the u64 difference rendered signed through numeric_in.
pub fn pg_lsn_mi(lsn1: XLogRecPtr, lsn2: XLogRecPtr) -> PgResult<NumericImage> {
    if lsn1 < lsn2 {
        let mut s = String::from("-");
        s.push_str(&(lsn2 - lsn1).to_string());
        Ok(adt_numeric::numeric_in(&s, -1, None)?.expect("hard-error path returns Err"))
    } else {
        u64_to_numeric(lsn1 - lsn2)
    }
}

#[track_caller]
#[cold]
fn nan_arith(op: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot {op} NaN {} pg_lsn", if op == "add" { "to" } else { "from" }))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub fn numeric_pg_lsn(num: Num<'_>) -> PgResult<XLogRecPtr> {
    if num.is_special() {
        let what = if num.is_nan() { "NaN" } else { "infinity" };
        return Err(Box::new(
            PgError::error(format!("cannot convert {what} to {}", "pg_lsn"))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    let var = NumericVar::from_view(num.view());
    adt_numeric::var_to_uint64(var.view()).ok_or_else(|| {
        Box::new(
            PgError::error("pg_lsn out of range").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        )
    })
}

pub fn pg_lsn_pli(lsn: XLogRecPtr, nbytes: Num<'_>) -> PgResult<XLogRecPtr> {
    if nbytes.is_nan() {
        return Err(nan_arith("add"));
    }
    let num = u64_to_numeric(lsn)?;
    let res = adt_numeric::numeric_add_common(num.num(), nbytes)?;
    numeric_pg_lsn(res.num())
}

pub fn pg_lsn_mii(lsn: XLogRecPtr, nbytes: Num<'_>) -> PgResult<XLogRecPtr> {
    if nbytes.is_nan() {
        return Err(nan_arith("subtract"));
    }
    let num = u64_to_numeric(lsn)?;
    let res = adt_numeric::numeric_sub_common(num.num(), nbytes)?;
    numeric_pg_lsn(res.num())
}
