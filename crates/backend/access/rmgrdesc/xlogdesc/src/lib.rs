//! Port of `src/backend/access/rmgrdesc/xlogdesc.c` — rmgr descriptor routines
//! for `access/transam/xlog.c` (the XLOG resource manager).
//!
//! [`xlog_desc`] appends a human-readable rendering of one XLOG WAL record to
//! the caller's `StringInfo`; [`xlog_identify`] names the record subtype. The C
//! signature `void xlog_desc(StringInfo buf, XLogReaderState *record)` becomes
//! `fn xlog_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) ->
//! PgResult<()>`, mirroring the sibling `backend-access-rmgrdesc-*` crates:
//!
//! - `buf` is the caller's context-allocated string; appends are fallible
//!   because C's `appendStringInfo` can `ereport(ERROR)` on OOM.
//! - `record.info()` is `XLogRecGetInfo(record)` (masked `& ~XLR_INFO_MASK`
//!   here exactly where the C masks) and `record.main_data()` is
//!   `XLogRecGetData(record)`.
//! - Each `CheckPoint` / `xl_*` payload is parsed from the raw bytes at the
//!   exact C struct field offsets by [`wal`]'s bounds-checked
//!   `from_bytes` (native byte order == the C struct cast).
//!
//! This file also owns the `wal_level_options` GUC enum table and the private
//! `get_wal_level_string`, both ported here.
//!
//! # External dependency
//!
//! `xlog_desc` calls one routine it does not own: `timestamptz_to_str`
//! (`utils/adt/timestamp.c`), for `END_OF_RECOVERY` / `OVERWRITE_CONTRECORD`,
//! reached through the owner's per-owner seam.
//!
//! # `XLogRecGetBlockRefInfo`
//!
//! `xlogdesc.c` also defines the block-reference formatter
//! `XLogRecGetBlockRefInfo` (used by `pg_waldump` and `xlogrecovery.c`'s
//! WAL_DEBUG dump). It is not an `rm_desc`/`rm_identify` rmgr slot — it has no
//! seam and no in-repo consumer (`xlogrecovery::desc::xlog_block_info` is a
//! pending desc-family stub), and it requires the detailed FPI block accessors
//! (`XLogRecGetBlock(record, id)->bimg_info/hole_offset/...`) which
//! `DecodedXLogRecord` does not yet expose publicly. It is therefore not part
//! of this unit's installed contract and is deferred until those accessors and
//! a consumer land.

#![allow(non_upper_case_globals)]

extern crate alloc;

use ::mcx::PgString;
use types_core::{uint8, TimestampTz, XLogRecPtr};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use wal::{
    xl_end_of_recovery, xl_overwrite_contrecord, xl_parameter_change, xl_restore_point, CheckPoint,
    DecodedXLogRecord, XLR_INFO_MASK,
};

use timestamp_seams as timestamp_seams;

// XLOG info values for the XLOG rmgr (`catalog/pg_control.h`).
pub const XLOG_CHECKPOINT_SHUTDOWN: uint8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE: uint8 = 0x10;
pub const XLOG_NOOP: uint8 = 0x20;
pub const XLOG_NEXTOID: uint8 = 0x30;
pub const XLOG_SWITCH: uint8 = 0x40;
pub const XLOG_BACKUP_END: uint8 = 0x50;
pub const XLOG_PARAMETER_CHANGE: uint8 = 0x60;
pub const XLOG_RESTORE_POINT: uint8 = 0x70;
pub const XLOG_FPW_CHANGE: uint8 = 0x80;
pub const XLOG_END_OF_RECOVERY: uint8 = 0x90;
pub const XLOG_FPI_FOR_HINT: uint8 = 0xA0;
pub const XLOG_FPI: uint8 = 0xB0;
/* 0xC0 is used in Postgres 9.5-11 */
pub const XLOG_OVERWRITE_CONTRECORD: uint8 = 0xD0;
pub const XLOG_CHECKPOINT_REDO: uint8 = 0xE0;

// `WalLevel` enum int values (access/xlog.h), as used by the GUC table.
const WAL_LEVEL_MINIMAL: i32 = 0;
const WAL_LEVEL_REPLICA: i32 = 1;
const WAL_LEVEL_LOGICAL: i32 = 2;

/// `wal_level_options[]` — the GUC enum option table this file owns
/// (xlogdesc.c). Modeled as `(name, val, hidden)`; the trailing `None`
/// terminator mirrors the C `{NULL, 0, false}` sentinel so
/// [`get_wal_level_string`]'s loop bound is identical.
pub const wal_level_options: &[(Option<&str>, i32, bool)] = &[
    (Some("minimal"), WAL_LEVEL_MINIMAL, false),
    (Some("replica"), WAL_LEVEL_REPLICA, false),
    (Some("archive"), WAL_LEVEL_REPLICA, true), // deprecated
    (Some("hot_standby"), WAL_LEVEL_REPLICA, true), // deprecated
    (Some("logical"), WAL_LEVEL_LOGICAL, false),
    (None, 0, false),
];

/// `get_wal_level_string(int wal_level)` (xlogdesc.c) — find a string
/// representation for `wal_level`. Returns `"?"` if not found.
fn get_wal_level_string(wal_level: i32) -> &'static str {
    let mut wal_level_str = "?";

    // for (entry = wal_level_options; entry->name; entry++)
    for &(name, val, _hidden) in wal_level_options {
        let name = match name {
            Some(n) => n,
            None => break, // entry->name == NULL terminates the loop
        };
        if val == wal_level {
            wal_level_str = name;
            break;
        }
    }

    wal_level_str
}

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
fn lsn_format(lsn: XLogRecPtr) -> (u32, u32) {
    ((lsn >> 32) as u32, lsn as u32)
}

/// `EpochFromFullTransactionId(x)` — `(uint32) ((x).value >> 32)`.
fn epoch_from_full_transaction_id(value: u64) -> u32 {
    (value >> 32) as u32
}

/// `XidFromFullTransactionId(x)` — `(uint32) (x).value`.
fn xid_from_full_transaction_id(value: u64) -> u32 {
    value as u32
}

/// `timestamptz_to_str(dt)` (`utils/adt/timestamp.c`), through the owner's seam.
fn timestamptz_to_str(buf: &mut PgString<'_>, dt: TimestampTz) -> PgResult<()> {
    let s = timestamp_seams::timestamptz_to_str::call(buf.allocator(), dt)?;
    buf.try_push_str(s.as_str())
}

/// `xlog_desc(StringInfo buf, XLogReaderState *record)` (xlogdesc.c).
pub fn xlog_desc(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()> {
    let rec = record.main_data();
    let info = record.info() & !XLR_INFO_MASK;

    if info == XLOG_CHECKPOINT_SHUTDOWN || info == XLOG_CHECKPOINT_ONLINE {
        let checkpoint =
            CheckPoint::from_bytes(rec).ok_or_else(|| record_truncated("CheckPoint"))?;
        let (redo_hi, redo_lo) = lsn_format(checkpoint.redo());

        appendf!(
            buf,
            "redo {:X}/{:X}; \
             tli {}; prev tli {}; fpw {}; wal_level {}; xid {}:{}; oid {}; multi {}; offset {}; \
             oldest xid {} in DB {}; oldest multi {} in DB {}; \
             oldest/newest commit timestamp xid: {}/{}; \
             oldest running xid {}; {}",
            redo_hi,
            redo_lo,
            checkpoint.this_timeline_id(),
            checkpoint.prev_timeline_id(),
            if checkpoint.full_page_writes() {
                "true"
            } else {
                "false"
            },
            get_wal_level_string(checkpoint.wal_level()),
            epoch_from_full_transaction_id(checkpoint.next_xid()),
            xid_from_full_transaction_id(checkpoint.next_xid()),
            checkpoint.next_oid(),
            checkpoint.next_multi(),
            checkpoint.next_multi_offset(),
            checkpoint.oldest_xid(),
            checkpoint.oldest_xid_db(),
            checkpoint.oldest_multi(),
            checkpoint.oldest_multi_db(),
            checkpoint.oldest_commit_ts_xid(),
            checkpoint.newest_commit_ts_xid(),
            checkpoint.oldest_active_xid(),
            if info == XLOG_CHECKPOINT_SHUTDOWN {
                "shutdown"
            } else {
                "online"
            }
        )?;
    } else if info == XLOG_NEXTOID {
        // memcpy(&nextOid, rec, sizeof(Oid));
        let next_oid = read_u32(rec, 0, "Oid")?;
        appendf!(buf, "{}", next_oid)?;
    } else if info == XLOG_RESTORE_POINT {
        let xlrec =
            xl_restore_point::from_bytes(rec).ok_or_else(|| record_truncated("xl_restore_point"))?;
        // appendStringInfoString(buf, xlrec->rp_name);
        append_lossy(buf, xlrec.rp_name())?;
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        // no further information to print
    } else if info == XLOG_BACKUP_END {
        // memcpy(&startpoint, rec, sizeof(XLogRecPtr));
        let startpoint = read_u64(rec, 0, "XLogRecPtr")?;
        let (hi, lo) = lsn_format(startpoint);
        appendf!(buf, "{:X}/{:X}", hi, lo)?;
    } else if info == XLOG_PARAMETER_CHANGE {
        let xlrec = xl_parameter_change::from_bytes(rec)
            .ok_or_else(|| record_truncated("xl_parameter_change"))?;
        let wal_level_str = get_wal_level_string(xlrec.wal_level());

        appendf!(
            buf,
            "max_connections={} max_worker_processes={} \
             max_wal_senders={} max_prepared_xacts={} \
             max_locks_per_xact={} wal_level={} \
             wal_log_hints={} track_commit_timestamp={}",
            xlrec.max_connections(),
            xlrec.max_worker_processes(),
            xlrec.max_wal_senders(),
            xlrec.max_prepared_xacts(),
            xlrec.max_locks_per_xact(),
            wal_level_str,
            if xlrec.wal_log_hints() { "on" } else { "off" },
            if xlrec.track_commit_timestamp() {
                "on"
            } else {
                "off"
            }
        )?;
    } else if info == XLOG_FPW_CHANGE {
        // memcpy(&fpw, rec, sizeof(bool));
        let fpw = read_bool(rec, 0, "bool")?;
        buf.try_push_str(if fpw { "true" } else { "false" })?;
    } else if info == XLOG_END_OF_RECOVERY {
        let xlrec = xl_end_of_recovery::from_bytes(rec)
            .ok_or_else(|| record_truncated("xl_end_of_recovery"))?;
        appendf!(
            buf,
            "tli {}; prev tli {}; time ",
            xlrec.this_timeline_id(),
            xlrec.prev_timeline_id()
        )?;
        timestamptz_to_str(buf, xlrec.end_time())?;
        appendf!(buf, "; wal_level {}", get_wal_level_string(xlrec.wal_level()))?;
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        let xlrec = xl_overwrite_contrecord::from_bytes(rec)
            .ok_or_else(|| record_truncated("xl_overwrite_contrecord"))?;
        let (hi, lo) = lsn_format(xlrec.overwritten_lsn());
        appendf!(buf, "lsn {:X}/{:X}; time ", hi, lo)?;
        timestamptz_to_str(buf, xlrec.overwrite_time())?;
    } else if info == XLOG_CHECKPOINT_REDO {
        // memcpy(&wal_level, rec, sizeof(int));
        let wal_level = read_i32(rec, 0, "int")?;
        appendf!(buf, "wal_level {}", get_wal_level_string(wal_level))?;
    }

    Ok(())
}

/// `xlog_identify(uint8 info)` (xlogdesc.c).
pub fn xlog_identify(info: uint8) -> Option<&'static str> {
    match info & !XLR_INFO_MASK {
        XLOG_CHECKPOINT_SHUTDOWN => Some("CHECKPOINT_SHUTDOWN"),
        XLOG_CHECKPOINT_ONLINE => Some("CHECKPOINT_ONLINE"),
        XLOG_NOOP => Some("NOOP"),
        XLOG_NEXTOID => Some("NEXTOID"),
        XLOG_SWITCH => Some("SWITCH"),
        XLOG_BACKUP_END => Some("BACKUP_END"),
        XLOG_PARAMETER_CHANGE => Some("PARAMETER_CHANGE"),
        XLOG_RESTORE_POINT => Some("RESTORE_POINT"),
        XLOG_FPW_CHANGE => Some("FPW_CHANGE"),
        XLOG_END_OF_RECOVERY => Some("END_OF_RECOVERY"),
        XLOG_OVERWRITE_CONTRECORD => Some("OVERWRITE_CONTRECORD"),
        XLOG_FPI => Some("FPI"),
        XLOG_FPI_FOR_HINT => Some("FPI_FOR_HINT"),
        XLOG_CHECKPOINT_REDO => Some("CHECKPOINT_REDO"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Bounds-checked native-endian scalar reads for the `memcpy(&x, rec, sizeof)`
// records that carry a bare value rather than an `xl_*` struct.
// ---------------------------------------------------------------------------

fn read_u32(data: &[u8], offset: usize, what: &'static str) -> PgResult<u32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| record_truncated(what))?;
    Ok(u32::from_ne_bytes(bytes.try_into().expect("len 4")))
}

fn read_i32(data: &[u8], offset: usize, what: &'static str) -> PgResult<i32> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| record_truncated(what))?;
    Ok(i32::from_ne_bytes(bytes.try_into().expect("len 4")))
}

fn read_u64(data: &[u8], offset: usize, what: &'static str) -> PgResult<u64> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| record_truncated(what))?;
    Ok(u64::from_ne_bytes(bytes.try_into().expect("len 8")))
}

fn read_bool(data: &[u8], offset: usize, what: &'static str) -> PgResult<bool> {
    Ok(*data.get(offset).ok_or_else(|| record_truncated(what))? != 0)
}

/// `%s` over bytes not guaranteed UTF-8 (the restore-point name): stream them
/// into `buf` lossily through the fallible API.
fn append_lossy(buf: &mut PgString<'_>, bytes: &[u8]) -> PgResult<()> {
    for chunk in bytes.utf8_chunks() {
        buf.try_push_str(chunk.valid())?;
        if !chunk.invalid().is_empty() {
            buf.try_push(char::REPLACEMENT_CHARACTER)?;
        }
    }
    Ok(())
}

/// Adapter installed into the rmgr-table `xlog_desc` seam: extracts the decoded
/// record from the dispatcher's `XLogReaderState` (C's `record->record`) and
/// renders it.
pub fn xlog_desc_seam(
    buf: &mut PgString<'_>,
    record: &::wal::rmgr::XLogReaderState<'_>,
) -> PgResult<()> {
    let record = record
        .record
        .as_ref()
        .expect("xlog_desc called without a decoded record");
    xlog_desc(buf, record)
}

/// Install all seam slots owned by this crate.
pub fn init_seams() {
    xlogdesc_seams::xlog_desc::set(xlog_desc_seam);
    xlogdesc_seams::xlog_identify::set(xlog_identify);
}

#[cfg(test)]
mod tests;
