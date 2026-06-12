//! WAL resource-manager descriptor routines for the index/heap/transam rmgrs:
//! port of `src/backend/access/rmgrdesc/{brindesc,gindesc,gistdesc,hashdesc,
//! heapdesc,mxactdesc,nbtdesc,spgdesc,standbydesc}.c`.
//!
//! Each `*_desc` mirrors `void X_desc(StringInfo buf, XLogReaderState *record)`:
//! it appends the record description to the caller's buffer. The only failure
//! is `appendStringInfo`'s palloc out-of-memory `ereport(ERROR)`, surfaced as
//! `PgResult`. Record fields are read at the C struct offsets from the raw
//! record bytes (`XLogRecGetData` / `XLogRecGetBlockData` payloads); a record
//! shorter than the struct the C code casts it to panics (the C reads
//! garbage / faults there).

#![allow(non_upper_case_globals)]

pub mod brindesc;
pub mod gindesc;
pub mod gistdesc;
pub mod hashdesc;
pub mod heapdesc;
pub mod mxactdesc;
pub mod nbtdesc;
pub mod spgdesc;
pub mod standbydesc;

pub use brindesc::{brin_desc, brin_identify};
pub use gindesc::{gin_desc, gin_identify};
pub use gistdesc::{gist_desc, gist_identify};
pub use hashdesc::{hash_desc, hash_identify};
pub use heapdesc::{heap2_desc, heap2_identify, heap_desc, heap_identify,
                   heap_xlog_deserialize_prune_and_freeze, PruneFreezeSubRecords};
pub use mxactdesc::{multixact_desc, multixact_identify};
pub use nbtdesc::{btree_desc, btree_identify};
pub use spgdesc::{spg_desc, spg_identify};
pub use standbydesc::{standby_desc, standby_desc_invalidations, standby_identify};

use mcx::PgString;
use types_error::{PgError, PgResult};

/// `appendStringInfo(buf, fmt, ...)`: format straight into the buffer,
/// surfacing the allocation failure as the OOM `PgError` (not `fmt::Error`).
pub(crate) fn append(buf: &mut PgString<'_>, args: core::fmt::Arguments<'_>) -> PgResult<()> {
    struct Adapter<'a, 'mcx> {
        buf: &'a mut PgString<'mcx>,
        err: Option<PgError>,
    }
    impl core::fmt::Write for Adapter<'_, '_> {
        fn write_str(&mut self, s: &str) -> core::fmt::Result {
            self.buf.try_push_str(s).map_err(|e| {
                self.err = Some(e);
                core::fmt::Error
            })
        }
    }
    let mut a = Adapter { buf, err: None };
    if core::fmt::Write::write_fmt(&mut a, args).is_ok() {
        return Ok(());
    }
    let err = a.err.take();
    Err(err.unwrap_or_else(|| a.buf.allocator().oom(0)))
}

/// `appendStringInfo` with printf-style interpolation as a statement
/// (propagates the OOM error with `?`).
macro_rules! appendf {
    ($buf:expr, $($arg:tt)*) => {
        $crate::append($buf, core::format_args!($($arg)*))?
    };
}
pub(crate) use appendf;

const SHORT_RECORD: &str = "WAL record data shorter than the C struct it must hold";

pub(crate) fn u8_at(d: &[u8], off: usize) -> u8 {
    *d.get(off).expect(SHORT_RECORD)
}

pub(crate) fn i8_at(d: &[u8], off: usize) -> i8 {
    u8_at(d, off) as i8
}

pub(crate) fn bool_at(d: &[u8], off: usize) -> bool {
    u8_at(d, off) != 0
}

pub(crate) fn u16_at(d: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(d[off..off + 2].try_into().expect(SHORT_RECORD))
}

pub(crate) fn u32_at(d: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(d[off..off + 4].try_into().expect(SHORT_RECORD))
}

pub(crate) fn i32_at(d: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(d[off..off + 4].try_into().expect(SHORT_RECORD))
}

pub(crate) fn u64_at(d: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(d[off..off + 8].try_into().expect(SHORT_RECORD))
}

pub(crate) fn i64_at(d: &[u8], off: usize) -> i64 {
    i64::from_ne_bytes(d[off..off + 8].try_into().expect(SHORT_RECORD))
}

pub(crate) fn f64_at(d: &[u8], off: usize) -> f64 {
    f64::from_ne_bytes(d[off..off + 8].try_into().expect(SHORT_RECORD))
}

/// `BlockIdGetBlockNumber` of a `BlockIdData {bi_hi, bi_lo}` at `off`.
pub(crate) fn block_id_at(d: &[u8], off: usize) -> u32 {
    let hi = u16_at(d, off) as u32;
    let lo = u16_at(d, off + 2) as u32;
    (hi << 16) | lo
}

/// `EpochFromFullTransactionId` / `XidFromFullTransactionId`.
pub(crate) fn full_xid_parts(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// printf `%g` (default precision 6): shortest of `%e`/`%f` with trailing
/// zeros removed, scientific form when the decimal exponent is `< -4` or
/// `>= 6`.
pub(crate) struct GFmt(pub f64);

impl core::fmt::Display for GFmt {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let x = self.0;
        if x.is_nan() {
            return f.write_str("nan");
        }
        if x.is_infinite() {
            return f.write_str(if x < 0.0 { "-inf" } else { "inf" });
        }
        if x == 0.0 {
            return f.write_str(if x.is_sign_negative() { "-0" } else { "0" });
        }
        // 6 significant digits; take the exponent of the *rounded* value.
        let sci = format!("{:.5e}", x);
        let (mant, exp) = sci.split_once('e').expect("std e-format");
        let exp: i32 = exp.parse().expect("std e-format exponent");
        if exp < -4 || exp >= 6 {
            let mant = mant.trim_end_matches('0').trim_end_matches('.');
            write!(f, "{}e{}{:02}", mant, if exp < 0 { '-' } else { '+' }, exp.abs())
        } else {
            let prec = (5 - exp).max(0) as usize;
            let fixed = format!("{:.*}", prec, x);
            let fixed = if fixed.contains('.') {
                fixed.trim_end_matches('0').trim_end_matches('.')
            } else {
                fixed.as_str()
            };
            f.write_str(fixed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GFmt;

    #[test]
    fn gfmt_matches_printf_g() {
        assert_eq!(format!("{}", GFmt(0.0)), "0");
        assert_eq!(format!("{}", GFmt(1000.0)), "1000");
        assert_eq!(format!("{}", GFmt(80.0)), "80");
        assert_eq!(format!("{}", GFmt(0.5)), "0.5");
        assert_eq!(format!("{}", GFmt(0.0001)), "0.0001");
        assert_eq!(format!("{}", GFmt(0.00001)), "1e-05");
        assert_eq!(format!("{}", GFmt(1234567.0)), "1.23457e+06");
        assert_eq!(format!("{}", GFmt(123456.0)), "123456");
        assert_eq!(format!("{}", GFmt(1234.5678)), "1234.57");
        assert_eq!(format!("{}", GFmt(-2.5)), "-2.5");
    }
}
