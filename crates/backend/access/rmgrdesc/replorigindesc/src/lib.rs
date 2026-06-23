//! Port of `src/backend/access/rmgrdesc/replorigindesc.c` — rmgr descriptor
//! routines for `replication/logical/origin.c`.
//!
//! [`replorigin_desc`] appends a human-readable rendering of one replication
//! origin WAL record to the caller's `StringInfo`; [`replorigin_identify`]
//! names the record subtype. The C signature `void replorigin_desc(StringInfo
//! buf, XLogReaderState *record)` becomes `fn replorigin_desc(buf: &mut
//! PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()>`, mirroring the
//! sibling `backend-access-rmgrdesc-*` crates.
//!
//! Unlike most rmgr describers, `replorigindesc.c` switches on the *unmasked*
//! info byte (`switch (info)`, not `info & ~XLR_INFO_MASK`); the masking is
//! still applied to `info` once before the switch, exactly as the C does. The
//! `xl_replorigin_*` payloads are parsed by [`wal`]'s bounds-checked
//! `from_bytes`.

#![allow(non_upper_case_globals)]

extern crate alloc;

use ::mcx::PgString;
use ::types_core::uint8;
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use wal::{xl_replorigin_drop, xl_replorigin_set, DecodedXLogRecord, XLR_INFO_MASK};

/// `XLOG_REPLORIGIN_SET` (replication/origin.h).
pub const XLOG_REPLORIGIN_SET: uint8 = 0x00;
/// `XLOG_REPLORIGIN_DROP` (replication/origin.h).
pub const XLOG_REPLORIGIN_DROP: uint8 = 0x10;

/// The record payload is shorter than the record being read. Unreachable for
/// well-formed WAL; loud `ERRCODE_DATA_CORRUPTED` beats reading garbage.
fn record_truncated(what: &'static str) -> PgError {
    PgError::error(alloc::format!("WAL record payload too short for {what}"))
        .with_sqlstate(ERRCODE_DATA_CORRUPTED)
}

/// `appendStringInfo(buf, fmt, ...)`: format into the caller's string,
/// surfacing an allocation failure as the context's OOM `PgError`.
fn append(buf: &mut PgString<'_>, args: core::fmt::Arguments<'_>) -> PgResult<()> {
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

macro_rules! appendf {
    ($buf:expr, $($arg:tt)*) => {
        $crate::append($buf, core::format_args!($($arg)*))
    };
}

/// `LSN_FORMAT_ARGS(lsn)` — `(uint32) ((lsn) >> 32), (uint32) (lsn)`,
/// rendered with the C `%X/%X` (uppercase hex, no leading zeros).
fn lsn_format(lsn: u64) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

/// `replorigin_desc(StringInfo buf, XLogReaderState *record)` (replorigindesc.c).
pub fn replorigin_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    match info {
        XLOG_REPLORIGIN_SET => {
            let xlrec = xl_replorigin_set::from_bytes(data)
                .ok_or_else(|| record_truncated("xl_replorigin_set"))?;
            let (hi, lo) = lsn_format(xlrec.remote_lsn());
            appendf!(
                buf,
                "set {}; lsn {:X}/{:X}; force: {}",
                xlrec.node_id(),
                hi,
                lo,
                // C `%d` over a `bool` field: 0 / 1.
                xlrec.force() as i32
            )?;
        }
        XLOG_REPLORIGIN_DROP => {
            let xlrec = xl_replorigin_drop::from_bytes(data)
                .ok_or_else(|| record_truncated("xl_replorigin_drop"))?;
            appendf!(buf, "drop {}", xlrec.node_id())?;
        }
        _ => {}
    }

    Ok(())
}

/// `replorigin_identify(uint8 info)` (replorigindesc.c). The C switches on the
/// raw `info` byte (no `& ~XLR_INFO_MASK`).
pub fn replorigin_identify(info: uint8) -> Option<&'static str> {
    match info {
        XLOG_REPLORIGIN_SET => Some("SET"),
        XLOG_REPLORIGIN_DROP => Some("DROP"),
        _ => None,
    }
}

/// Adapter installed into the rmgr-table `replorigin_desc` seam: extracts the
/// decoded record from the dispatcher's `XLogReaderState` (C's
/// `record->record`) and renders it.
pub fn replorigin_desc_seam(
    buf: &mut PgString<'_>,
    record: &::wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("replorigin_desc called without a decoded record");
    replorigin_desc(buf, record)
}

/// Install all seam slots owned by this crate.
pub fn init_seams() {
    replorigindesc_seams::replorigin_desc::set(replorigin_desc_seam);
    replorigindesc_seams::replorigin_identify::set(replorigin_identify);
}

#[cfg(test)]
mod tests;
