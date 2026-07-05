//! rm_desc/rm_identify renderers; pg_waldump output is the differential
//! oracle, so format strings are C-exact. timestamptz_to_str mirrors C's
//! backend/frontend link seam (waldesc installs the compat.c flavor).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use core::cell::Cell;
use stringinfo::StringInfo;
use types_core::TimestampTz;
use types_error::{PgError, PgResult};
use xlogreader_seams::XLogReaderState;

pub mod brindesc;
pub mod spgdesc;
pub mod clogdesc;
pub mod committsdesc;
pub mod dbasedesc;
pub mod genericdesc;
pub mod gindesc;
pub mod gistdesc;
pub mod hashdesc;
pub mod heapdesc;
pub mod logicalmsgdesc;
pub mod mxactdesc;
pub mod nbtdesc;
pub mod relmapdesc;
pub mod replorigindesc;
pub mod seqdesc;
pub mod smgrdesc;
pub mod standbydesc;
pub mod tblspcdesc;
pub mod xactdesc;
pub mod xlogdesc;

pub(crate) struct FmtSink<'a, 'b, 'mcx> {
    buf: &'a mut StringInfo<'mcx>,
    err: &'b mut Option<Box<PgError>>,
}

impl core::fmt::Write for FmtSink<'_, '_, '_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        match self.buf.append_str(s) {
            Ok(()) => Ok(()),
            Err(e) => {
                *self.err = Some(e);
                Err(core::fmt::Error)
            }
        }
    }
}

pub(crate) fn append_args(
    buf: &mut StringInfo<'_>,
    args: core::fmt::Arguments<'_>,
) -> PgResult<()> {
    let mut err = None;
    let r = core::fmt::write(&mut FmtSink { buf, err: &mut err }, args);
    match err {
        Some(e) => Err(e),
        None => {
            debug_assert!(r.is_ok());
            Ok(())
        }
    }
}

macro_rules! appendf {
    ($buf:expr, $($arg:tt)*) => {
        $crate::append_args($buf, core::format_args!($($arg)*))
    };
}
pub(crate) use appendf;

#[cold]
pub(crate) fn record_truncated(what: &'static str) -> Box<PgError> {
    Box::new(PgError::error(format!("WAL record payload too short for {what}")))
}

// Bounds-checked native-endian view of a record payload (the C struct cast).
#[derive(Clone, Copy)]
pub(crate) struct Rec<'a>(pub &'a [u8]);

impl Rec<'_> {
    pub fn u8(&self, off: usize, what: &'static str) -> PgResult<u8> {
        self.0.get(off).copied().ok_or_else(|| record_truncated(what))
    }
    pub fn u16(&self, off: usize, what: &'static str) -> PgResult<u16> {
        Ok(u16::from_ne_bytes(self.arr::<2>(off, what)?))
    }
    pub fn u32(&self, off: usize, what: &'static str) -> PgResult<u32> {
        Ok(u32::from_ne_bytes(self.arr::<4>(off, what)?))
    }
    pub fn i32(&self, off: usize, what: &'static str) -> PgResult<i32> {
        Ok(i32::from_ne_bytes(self.arr::<4>(off, what)?))
    }
    pub fn u64(&self, off: usize, what: &'static str) -> PgResult<u64> {
        Ok(u64::from_ne_bytes(self.arr::<8>(off, what)?))
    }
    pub fn i64(&self, off: usize, what: &'static str) -> PgResult<i64> {
        Ok(i64::from_ne_bytes(self.arr::<8>(off, what)?))
    }
    pub fn arr<const N: usize>(&self, off: usize, what: &'static str) -> PgResult<[u8; N]> {
        self.0
            .get(off..off + N)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| record_truncated(what))
    }
}

// XLogRecGetData: main_data points into the reader's decode buffer, valid for
// the desc callback's duration (same contract as rmgr::xact_redo).
pub(crate) fn rec_data<'r>(record: &'r XLogReaderState) -> &'r [u8] {
    match record.record.as_ref() {
        // SAFETY: see above.
        Some(r) => unsafe { r.main_data_bytes() },
        None => &[],
    }
}

pub(crate) fn rec_info(record: &XLogReaderState) -> u8 {
    record.record.as_ref().map_or(0, |r| r.xl_info)
}

pub(crate) fn has_block_data(record: &XLogReaderState, block_id: u8) -> bool {
    record.has_block_ref(block_id) && record.block(block_id).has_data
}

pub(crate) fn block_data<'r>(record: &'r XLogReaderState, block_id: u8) -> &'r [u8] {
    if !has_block_data(record, block_id) {
        return &[];
    }
    // SAFETY: see rec_data.
    unsafe { record.block(block_id).data_bytes() }
}

// XLR_INFO_MASK (xlogrecord.h); local copy, transam_xlog would cycle via rmgr.
pub(crate) const XLR_INFO_MASK: u8 = 0x0F;

pub const MAXDATELEN: usize = adt_datetime::MAXDATELEN;

pub type TimestamptzToStrFn = fn(TimestampTz, &mut [u8; MAXDATELEN + 1]) -> usize;

thread_local! {
    static TIMESTAMPTZ_TO_STR: Cell<TimestamptzToStrFn> =
        const { Cell::new(backend_timestamptz_to_str) };
}

pub fn install_timestamptz_to_str(f: TimestamptzToStrFn) {
    TIMESTAMPTZ_TO_STR.with(|c| c.set(f));
}

fn backend_timestamptz_to_str(t: TimestampTz, buf: &mut [u8; MAXDATELEN + 1]) -> usize {
    use adt_datetime::consts::{fsec_t, pg_tm};
    use adt_timestamp::{timestamp2tm, EncodeSpecialTimestamp, TIMESTAMP_NOT_FINITE};
    if TIMESTAMP_NOT_FINITE(t) {
        return EncodeSpecialTimestamp(t, &mut buf[..]);
    }
    let mut tz: i32 = 0;
    let mut tm = pg_tm::default();
    let mut fsec: fsec_t = 0;
    let mut tzn: Option<&'static str> = None;
    if timestamp2tm(t, Some(&mut tz), &mut tm, &mut fsec, Some(&mut tzn), None).is_ok() {
        adt_datetime::EncodeDateTime(
            &mut tm,
            fsec,
            true,
            tz,
            tzn.map(str::as_bytes),
            adt_datetime::USE_ISO_DATES,
            &mut buf[..],
        )
    } else {
        let msg = b"(timestamp out of range)";
        buf[..msg.len()].copy_from_slice(msg);
        msg.len()
    }
}

pub(crate) fn append_timestamptz(buf: &mut StringInfo<'_>, t: TimestampTz) -> PgResult<()> {
    let mut tmp = [0u8; MAXDATELEN + 1];
    let n = TIMESTAMPTZ_TO_STR.with(|c| c.get())(t, &mut tmp);
    buf.append_bytes(&tmp[..n])
}

pub(crate) fn array_desc(
    buf: &mut StringInfo<'_>,
    count: usize,
    mut elem_desc: impl FnMut(&mut StringInfo<'_>, usize) -> PgResult<()>,
) -> PgResult<()> {
    if count == 0 {
        return buf.append_str(" []");
    }
    buf.append_str(" [")?;
    for i in 0..count {
        elem_desc(buf, i)?;
        if i < count - 1 {
            buf.append_str(", ")?;
        }
    }
    buf.append_byte(b']')
}

pub fn init_seams() {}
