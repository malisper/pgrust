//! Port of `src/backend/access/transam/xlogbackup.c` (PostgreSQL 18.3).
//!
//! xlogbackup.c holds the one internal helper used to shape a base backup's
//! on-disk metadata: [`build_backup_content`] assembles the text of a
//! `backup_label` file (or, for `ishistoryfile`, a backup history file) from a
//! [`BackupState`] snapshot. The C routine returns a `palloc`'d `char *` built
//! up with `appendStringInfo`; this port returns an owned `Vec<u8>`.
//!
//! ## Why `Vec<u8>` rather than `String`
//!
//! The `LABEL: %s` line copies `state->name` verbatim. `state->name` is a
//! `char[MAXPGPATH + 1]` (xlogbackup.h:25) holding the backup label in the
//! server encoding, length-checked but never UTF-8-validated upstream. A label
//! containing high bytes under LATIN1/EUC_JP/SQL_ASCII must be emitted
//! byte-for-byte exactly as the C `printf` does, so the buffer is accumulated
//! as raw bytes; UTF-8 validation here would diverge by erroring on valid
//! server-encoding input.
//!
//! ## Externals
//!
//! C reads the globals `wal_segment_size` and `log_timezone`. `wal_segment_size`
//! is threaded in as an explicit parameter (with a [`build_backup_content_default`]
//! convenience wrapper that supplies the default segment size and the process
//! `log_timezone` from `state-pgtz`). The two genuine cross-crate calls —
//! `pg_localtime` and `pg_strftime` — resolve to the real timezone crates
//! (`backend-timezone-localtime` / `backend-timezone-strftime`), depended on
//! directly, so this crate needs **no** seams. The `XLByteToSeg` /
//! `XLogFileName` uses are C macros from `xlog_internal.h` (pure segment
//! arithmetic with no cross-crate dependency); they are ported in-crate below,
//! identical to the `backend-access-transam-xlog` definitions.

use std::ffi::CStr;

use localtime::{pg_localtime, pg_tz};
use strftime::pg_strftime;
use utils_error::{PgError, PgResult};
use types_core::{pg_time_t, TimeLineID, XLogRecPtr, XLogSegNo};
use wal::{BackupState, DEFAULT_XLOG_SEG_SIZE};

/// `XLogSegmentsPerXLogId` (`xlog_internal.h`): how many WAL segments make up
/// one logical 4 GiB "log id" for a given segment size.
fn xlog_segments_per_xlog_id(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000_u64 / wal_segsz_bytes as u64
}

/// `XLByteToSeg` (`xlog_internal.h`): the WAL segment number containing `xlrp`.
fn xl_byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

/// `XLogFileName` (`xlog_internal.h`): the 24-hex-digit WAL file name for a
/// `(tli, segno)` pair under the given segment size.
fn xlog_file_name(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segments = xlog_segments_per_xlog_id(wal_segsz_bytes);
    format!(
        "{tli:08X}{:08X}{:08X}",
        log_seg_no / segments,
        log_seg_no % segments
    )
}

/// Build the contents of a `backup_label` (or, when `ishistoryfile`, a backup
/// history) file.
///
/// 1:1 port of C `build_backup_content` (xlogbackup.c:28-94). Returns the
/// assembled bytes (the C `palloc`'d `char *` analog) as an owned `Vec<u8>`.
///
/// `wal_segment_size` and `log_timezone` correspond to the C globals of the
/// same names. `pg_localtime` returning `None` (a calendar-time conversion
/// failure) is surfaced as a recoverable error rather than the C
/// NULL-dereference.
pub fn build_backup_content(
    state: &BackupState,
    ishistoryfile: bool,
    wal_segment_size: i32,
    log_timezone: &pg_tz,
) -> PgResult<Vec<u8>> {
    // Use the log timezone here, not the session timezone (xlogbackup.c:39-41).
    let startstrbuf = format_backup_time(state.starttime(), log_timezone)?;

    let startsegno = xl_byte_to_seg(state.startpoint(), wal_segment_size);
    let startxlogfile = xlog_file_name(state.starttli(), startsegno, wal_segment_size);

    let mut result = Vec::new();
    append_lsn_line(
        &mut result,
        "START WAL LOCATION",
        state.startpoint(),
        Some(&startxlogfile),
    );

    if ishistoryfile {
        let stopsegno = xl_byte_to_seg(state.stoppoint(), wal_segment_size);
        let stopxlogfile = xlog_file_name(state.stoptli(), stopsegno, wal_segment_size);
        append_lsn_line(
            &mut result,
            "STOP WAL LOCATION",
            state.stoppoint(),
            Some(&stopxlogfile),
        );
    }

    append_lsn_line(&mut result, "CHECKPOINT LOCATION", state.checkpointloc(), None);
    result.extend_from_slice(b"BACKUP METHOD: streamed\n");
    result.extend_from_slice(b"BACKUP FROM: ");
    result.extend_from_slice(if state.started_in_recovery() {
        b"standby".as_slice()
    } else {
        b"primary".as_slice()
    });
    result.push(b'\n');
    result.extend_from_slice(b"START TIME: ");
    result.extend_from_slice(startstrbuf.as_slice());
    result.push(b'\n');
    // `LABEL: %s` — copy state->name verbatim (server-encoding bytes), matching
    // the C printf; do *not* UTF-8-validate (xlogbackup.c:65).
    result.extend_from_slice(b"LABEL: ");
    result.extend_from_slice(backup_name(state));
    result.push(b'\n');
    result.extend_from_slice(format!("START TIMELINE: {}\n", state.starttli()).as_bytes());

    if ishistoryfile {
        // Use the log timezone here, not the session timezone (xlogbackup.c:72-74).
        let stopstrfbuf = format_backup_time(state.stoptime(), log_timezone)?;
        result.extend_from_slice(b"STOP TIME: ");
        result.extend_from_slice(stopstrfbuf.as_slice());
        result.push(b'\n');
        result.extend_from_slice(format!("STOP TIMELINE: {}\n", state.stoptli()).as_bytes());
    }

    // Either both istartpoint and istarttli should be set, or neither
    // (xlogbackup.c:80-81). C asserts this; debug_assert mirrors the Assert.
    debug_assert_eq!(
        state.istartpoint() == 0,
        state.istarttli() == 0,
        "BackupState istartpoint/istarttli must be set together"
    );
    if state.istartpoint() != 0 {
        append_lsn_line(&mut result, "INCREMENTAL FROM LSN", state.istartpoint(), None);
        result
            .extend_from_slice(format!("INCREMENTAL FROM TLI: {}\n", state.istarttli()).as_bytes());
    }

    Ok(result)
}

/// Convenience wrapper using the default WAL segment size and the process log
/// timezone, matching how C reads its `wal_segment_size` / `log_timezone`
/// globals at the call site.
pub fn build_backup_content_default(state: &BackupState, ishistoryfile: bool) -> PgResult<Vec<u8>> {
    let log_timezone = state_pgtz::log_timezone();
    build_backup_content(
        state,
        ishistoryfile,
        DEFAULT_XLOG_SEG_SIZE,
        &log_timezone,
    )
}

/// `appendStringInfo(result, "<label>: %X/%X[ (file %s)]\n", LSN_FORMAT_ARGS(lsn), file)`.
///
/// `LSN_FORMAT_ARGS(lsn)` expands to `(uint32)((lsn) >> 32), (uint32)(lsn)`
/// (xlogdefs.h), i.e. the high and low 32-bit halves printed in uppercase hex.
fn append_lsn_line(out: &mut Vec<u8>, label: &str, lsn: XLogRecPtr, file: Option<&str>) {
    out.extend_from_slice(label.as_bytes());
    out.extend_from_slice(b": ");
    out.extend_from_slice(format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32).as_bytes());
    if let Some(file) = file {
        out.extend_from_slice(b" (file ");
        out.extend_from_slice(file.as_bytes());
        out.push(b')');
    }
    out.push(b'\n');
}

/// Return the backup label bytes up to the NUL terminator, verbatim.
///
/// C emits `state->name` through `appendStringInfo(result, "LABEL: %s\n", ...)`
/// (xlogbackup.c:65), copying the server-encoding bytes up to the NUL without
/// any encoding validation. We slice the raw `[u8; MAXPGPATH + 1]` at the first
/// NUL (or the full array if unterminated) so non-UTF-8 labels survive
/// byte-for-byte.
fn backup_name(state: &BackupState) -> &[u8] {
    let name = state.name();
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    &name[..end]
}

/// `pg_strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S %Z", pg_localtime(&t, tz))`
/// (xlogbackup.c:40-41 / 73-74) into a 128-byte stack buffer, returning the
/// formatted bytes.
///
/// C never NULL-checks `pg_localtime` here; the Rust `pg_localtime` returns
/// `Option`, so a conversion failure becomes a recoverable error. Likewise
/// `pg_strftime` returns `None` if the output overflows the buffer.
fn format_backup_time(time: pg_time_t, tz: &pg_tz) -> PgResult<Vec<u8>> {
    let tm = pg_localtime(time, tz).ok_or_else(|| PgError::error("time value out of range"))?;

    // C uses a `char startstrbuf[128]` (xlogbackup.c:31) / `char stopstrfbuf[128]`
    // (xlogbackup.c:70); mirror the fixed-size buffer so the same overflow bound
    // applies.
    let mut buf = [0u8; 128];
    let format = CStr::from_bytes_with_nul(b"%Y-%m-%d %H:%M:%S %Z\0")
        .expect("static format literal is NUL-terminated");
    let len = pg_strftime(&mut buf, format, &tm)
        .ok_or_else(|| PgError::error("could not format backup timestamp"))?;

    Ok(buf[..len].to_vec())
}

#[cfg(test)]
mod tests;
