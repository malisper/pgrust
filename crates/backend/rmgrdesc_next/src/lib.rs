//! WAL resource-manager descriptor routines for the index/heap/transam rmgrs:
//! port of `src/backend/access/rmgrdesc/{brindesc,gindesc,gistdesc,hashdesc,
//! heapdesc,mxactdesc,nbtdesc,spgdesc,standbydesc}.c`.
//!
//! Each `*_desc` mirrors `void X_desc(StringInfo buf, XLogReaderState *record)`:
//! it appends the record description to the caller's buffer. The only failure
//! is `appendStringInfo`'s palloc out-of-memory `ereport(ERROR)`, surfaced as
//! `PgResult`. Record bodies decode through the `xl_*` structs in
//! `types-xlog-records` (shared with the future redo ports); a record shorter
//! than the struct the C code casts it to panics (the C reads garbage /
//! faults there).

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
use wal::rmgr::XLogReaderState;

/// Install all seams owned by this crate. Called once at startup from
/// `init::init_all()`.
pub fn init_seams() {
    use brindesc_seams as brindesc_seams;
    use gindesc_seams as gindesc_seams;
    use gistdesc_seams as gistdesc_seams;
    use hashdesc_seams as hashdesc_seams;
    use heapdesc_seams as heapdesc_seams;
    use mxactdesc_seams as mxactdesc_seams;
    use nbtdesc_seams as nbtdesc_seams;
    use spgdesc_seams as spgdesc_seams;
    use standbydesc_seams as standbydesc_seams;

    // Adapter shims: the seams expose the RmDesc signature
    // fn(buf: &mut PgString<'_>, record: &XLogReaderState<'_>) -> PgResult<()>
    // while the implementations take &DecodedXLogRecord<'_>.
    // The C rm_desc callbacks access the record via state->record, so we
    // unwrap the Option<DecodedXLogRecord> field (None is a programming error
    // equivalent to a NULL-deref in C).
    fn brin_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        brindesc::brin_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn gin_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        gindesc::gin_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn gist_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        gistdesc::gist_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn hash_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        hashdesc::hash_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn heap_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        heapdesc::heap_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn heap2_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        heapdesc::heap2_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn multixact_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        mxactdesc::multixact_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn btree_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        nbtdesc::btree_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn spg_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        spgdesc::spg_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }
    fn standby_desc_shim(buf: &mut PgString<'_>, state: &XLogReaderState<'_>) -> types_error::PgResult<()> {
        standbydesc::standby_desc(buf, state.record.as_ref().expect("rm_desc: state->record is NULL"))
    }

    brindesc_seams::brin_desc::set(brin_desc_shim);
    brindesc_seams::brin_identify::set(brindesc::brin_identify);
    gindesc_seams::gin_desc::set(gin_desc_shim);
    gindesc_seams::gin_identify::set(gindesc::gin_identify);
    gistdesc_seams::gist_desc::set(gist_desc_shim);
    gistdesc_seams::gist_identify::set(gistdesc::gist_identify);
    hashdesc_seams::hash_desc::set(hash_desc_shim);
    hashdesc_seams::hash_identify::set(hashdesc::hash_identify);
    heapdesc_seams::heap_desc::set(heap_desc_shim);
    heapdesc_seams::heap_identify::set(heapdesc::heap_identify);
    heapdesc_seams::heap2_desc::set(heap2_desc_shim);
    heapdesc_seams::heap2_identify::set(heapdesc::heap2_identify);
    mxactdesc_seams::multixact_desc::set(multixact_desc_shim);
    mxactdesc_seams::multixact_identify::set(mxactdesc::multixact_identify);
    nbtdesc_seams::btree_desc::set(btree_desc_shim);
    nbtdesc_seams::btree_identify::set(nbtdesc::btree_identify);
    spgdesc_seams::spg_desc::set(spg_desc_shim);
    spgdesc_seams::spg_identify::set(spgdesc::spg_identify);
    standbydesc_seams::standby_desc::set(standby_desc_shim);
    standbydesc_seams::standby_identify::set(standbydesc::standby_identify);
}

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

/// Raw reads for the few genuinely byte-oriented walks (GIN segment actions,
/// the multixact page numbers); record-body structs decode through
/// `types-xlog-records` instead.
pub(crate) fn u8_at(d: &[u8], off: usize) -> u8 {
    *d.get(off).expect(SHORT_RECORD)
}

pub(crate) fn u16_at(d: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(d[off..off + 2].try_into().expect(SHORT_RECORD))
}

pub(crate) fn i64_at(d: &[u8], off: usize) -> i64 {
    i64::from_ne_bytes(d[off..off + 8].try_into().expect(SHORT_RECORD))
}

/// A fixed-capacity stack buffer for intermediate number formatting — the C
/// counterpart formats `%g` straight into the palloc'd StringInfo, so this
/// path must not touch the global allocator.
struct StackStr<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> StackStr<N> {
    const fn new() -> Self {
        Self { buf: [0; N], len: 0 }
    }

    fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).expect("fmt output is UTF-8")
    }
}

impl<const N: usize> core::fmt::Write for StackStr<N> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let bytes = s.as_bytes();
        if self.len + bytes.len() > N {
            return Err(core::fmt::Error);
        }
        self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(())
    }
}

/// printf `%g` (default precision 6): shortest of `%e`/`%f` with trailing
/// zeros removed, scientific form when the decimal exponent is `< -4` or
/// `>= 6`. Formats via fixed stack buffers (the widest intermediate is
/// `-999999.999999999`, well under [`GFMT_BUF`] bytes).
pub(crate) struct GFmt(pub f64);

/// Stack-buffer capacity for [`GFmt`] intermediates.
const GFMT_BUF: usize = 32;

impl core::fmt::Display for GFmt {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        use core::fmt::Write;

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
        let mut sci = StackStr::<GFMT_BUF>::new();
        write!(sci, "{:.5e}", x)?;
        let (mant, exp) = sci.as_str().split_once('e').expect("std e-format");
        let exp: i32 = exp.parse().expect("std e-format exponent");
        if exp < -4 || exp >= 6 {
            let mant = mant.trim_end_matches('0').trim_end_matches('.');
            write!(f, "{}e{}{:02}", mant, if exp < 0 { '-' } else { '+' }, exp.abs())
        } else {
            let prec = (5 - exp).max(0) as usize;
            let mut fixed = StackStr::<GFMT_BUF>::new();
            write!(fixed, "{:.*}", prec, x)?;
            let s = fixed.as_str();
            let s = if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.')
            } else {
                s
            };
            f.write_str(s)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use mcx::{slice_in, Mcx};
    use wal::{DecodedBkpBlock, DecodedXLogRecord, XLogRecord};

    /// Build a `DecodedXLogRecord` carrying just the pieces the desc
    /// routines read: `xl_info`, the main data, and the block references.
    pub(crate) fn record<'a>(
        mcx: Mcx<'a>,
        info: u8,
        data: &'a [u8],
        blocks: &[DecodedBkpBlock<'a>],
    ) -> DecodedXLogRecord<'a> {
        DecodedXLogRecord::new(
            XLogRecord::new(0, 0, 0, info, 0, 0),
            data,
            slice_in(mcx, blocks).unwrap(),
        )
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
        assert_eq!(format!("{}", GFmt(f64::MIN_POSITIVE / 4.0)), "5.56268e-309");
        assert_eq!(format!("{}", GFmt(-f64::MAX)), "-1.79769e+308");
    }
}
