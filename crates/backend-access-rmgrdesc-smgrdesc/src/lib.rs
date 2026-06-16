//! Port of `src/backend/access/rmgrdesc/smgrdesc.c` — rmgr descriptor routines
//! for `catalog/storage.c`.
//!
//! [`smgr_desc`] appends a human-readable rendering of one SMGR WAL record to
//! the caller's `StringInfo`; [`smgr_identify`] names the record subtype. The C
//! signature `void smgr_desc(StringInfo buf, XLogReaderState *record)` becomes
//! `fn smgr_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) ->
//! PgResult<()>`, mirroring the sibling `backend-access-rmgrdesc-*` crates:
//!
//! - `buf` is the caller's context-allocated string; appends are fallible
//!   because C's `appendStringInfo` can `ereport(ERROR)` on OOM.
//! - `record.info()` is `XLogRecGetInfo(record)` (masked `& ~XLR_INFO_MASK`
//!   here exactly where the C masks) and `record.main_data()` is
//!   `XLogRecGetData(record)`.
//! - The `xl_smgr_*` payloads are parsed by [`types_wal`]'s bounds-checked
//!   `from_bytes`.
//!
//! # External dependency
//!
//! `smgr_desc` calls one routine it does not own: `relpathperm()`
//! (`common/relpath.h` macro over `relpathbackend(.., INVALID_PROC_NUMBER,
//! ..)`), reached through the owner's per-owner seam (`common-relpath-seams`).

#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::string::String;

use mcx::PgString;
use types_core::{uint8, ForkNumber, INVALID_PROC_NUMBER};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use types_storage::RelFileLocator;
use types_wal::{xl_smgr_create, xl_smgr_truncate, DecodedXLogRecord, XLR_INFO_MASK};

use common_relpath_seams as relpath_seams;

/// `XLOG_SMGR_CREATE` (catalog/storage_xlog.h) — "XLOG gives us high 4 bits".
pub const XLOG_SMGR_CREATE: uint8 = 0x10;
/// `XLOG_SMGR_TRUNCATE` (catalog/storage_xlog.h).
pub const XLOG_SMGR_TRUNCATE: uint8 = 0x20;

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

/// `relpathperm(rlocator, fork).str` (`common/relpath.h` macro over
/// `relpathbackend(.., INVALID_PROC_NUMBER, ..)`), reached through the owner's
/// per-owner seam.
fn relpathperm(rlocator: RelFileLocator, fork: ForkNumber) -> String {
    relpath_seams::relpathbackend::call(rlocator, INVALID_PROC_NUMBER, fork)
}

/// `smgr_desc(StringInfo buf, XLogReaderState *record)` (smgrdesc.c).
pub fn smgr_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let data = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_SMGR_CREATE {
        let xlrec =
            xl_smgr_create::from_bytes(data).ok_or_else(|| record_truncated("xl_smgr_create"))?;
        let path = relpathperm(xlrec.rlocator(), xlrec.fork_num());
        buf.try_push_str(&path)?;
    } else if info == XLOG_SMGR_TRUNCATE {
        let xlrec = xl_smgr_truncate::from_bytes(data)
            .ok_or_else(|| record_truncated("xl_smgr_truncate"))?;
        let path = relpathperm(xlrec.rlocator(), ForkNumber::MAIN_FORKNUM);
        appendf!(
            buf,
            "{} to {} blocks flags {}",
            path,
            xlrec.blkno(),
            xlrec.flags()
        )?;
    }

    Ok(())
}

/// `smgr_identify(uint8 info)` (smgrdesc.c).
pub fn smgr_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_SMGR_CREATE => Some("CREATE"),
        XLOG_SMGR_TRUNCATE => Some("TRUNCATE"),
        _ => None,
    }
}

/// Adapter installed into the rmgr-table `smgr_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it. The reader is always positioned on a decoded record when the
/// rmgr table invokes `rm_desc`.
pub fn smgr_desc_seam(
    buf: &mut PgString<'_>,
    record: &types_wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("smgr_desc called without a decoded record");
    smgr_desc(buf, record)
}

/// Install all seam slots owned by this crate.
pub fn init_seams() {
    backend_access_rmgrdesc_smgrdesc_seams::smgr_desc::set(smgr_desc_seam);
    backend_access_rmgrdesc_smgrdesc_seams::smgr_identify::set(smgr_identify);
}

#[cfg(test)]
mod tests;
