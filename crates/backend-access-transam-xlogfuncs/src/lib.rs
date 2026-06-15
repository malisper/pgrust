//! PostgreSQL 18.3 `src/backend/access/transam/xlogfuncs.c` — the WAL control
//! and information SQL-callable functions (`pg_backup_start`,
//! `pg_switch_wal`, `pg_current_wal_lsn`, `pg_walfile_name`,
//! `pg_wal_replay_pause`, `pg_promote`, …).
//!
//! Truth source: `postgres-18.3/src/backend/access/transam/xlogfuncs.c`.
//!
//! # Value model
//!
//! Like the sibling ported `adt`/SQL-callable crates (cf.
//! `backend-utils-adt-lsn-trigfuncs`, `backend-catalog-objectaddress`'s
//! `fmgr_sql`), these functions are ported as pure cores that take/return
//! decoded scalars rather than driving a live `fcinfo`:
//!
//! * `text` arguments arrive as `&str` (already decoded; the C
//!   `text_to_cstring(PG_GETARG_TEXT_PP(n))` is subsumed by the boundary).
//! * Scalar results (`pg_lsn` = [`XLogRecPtr`], `bool`, `timestamptz`) are
//!   returned directly; SQL NULL is `Option<_>::None`.
//! * Composite-row results are returned as a typed Rust struct, one field per
//!   output column ([`BackupStopResult`], [`WalfileNameOffset`],
//!   [`SplitWalfileName`]). The C `get_call_result_type(fcinfo, …)` assertion +
//!   `heap_form_tuple` + `HeapTupleGetDatum` have no value-boundary
//!   representation here — the caller (the future fmgr SQL-leg) drives the
//!   columns — so they are elided, exactly as `fmgr_sql.rs` does.
//!
//! This crate carries no `static`-mutable session backup state. The C
//! `backup_state` / `tablespace_map` / `backupcontext` file-scope statics are
//! the workhorses' (xlog.c `do_pg_backup_start`/`do_pg_backup_stop`) own
//! long-lived state in PG; in the value model the [`BackupState`] +
//! tablespace-map bytes flow start → caller → stop as explicit values, so the
//! statics collapse to no-ops here.
//!
//! # Externals / seams
//!
//! * `RecoveryInProgress` / `GetXLogWriteRecPtr` / `GetXLogInsertRecPtr` /
//!   `GetFlushRecPtr` / `GetWALInsertionTimeLine` and the file-name helpers
//!   (`XLByteToSeg`/`XLogFileName`/`XLogSegmentOffset`/`IsXLogFileName`/
//!   `XLogFromFileName`) are real functions in the ported `xlog` crate.
//! * `RequestXLogSwitch` / `XLogRestorePoint` are the xlog crate's deferred
//!   driver entries (panic until the WAL-write driver lands) — honest
//!   seam-and-panic.
//! * `XLogIsNeeded` / `XLogStandbyInfoActive` / `wal_segment_size` /
//!   `LogStandbySnapshot` cross via `backend-access-transam-xlog-seams`.
//! * `do_pg_backup_start` / `do_pg_backup_stop` / `get_backup_status` /
//!   `register_persistent_abort_backup_handler` are xlog.c workhorses declared
//!   in `xlog-seams` (this port adds the decls); xlog.c installs them when it
//!   exposes the backup machinery — until then they panic loudly.
//! * `GetXLogReplayRecPtr` / `WakeupRecovery` / `SetRecoveryPause` /
//!   `GetRecoveryPauseState` / `PromoteIsTriggered` / `GetLatestXTime` cross via
//!   `backend-access-transam-xlogrecovery-seams` (xlogrecovery.c not yet ported;
//!   they panic until it lands).
//! * `GetWalRcvFlushRecPtr` is the real ported `walreceiverfuncs` fn.
//! * `numeric_in` is the real ported `numeric` fn; `pg_lsn_mi` arithmetic is the
//!   real ported `pg_lsn` fn.
//! * `cstring_to_text` is the real ported `varlena` fn.
//! * `pg_promote` reaches the OS/latch substrate: `create_empty_file` /
//!   `unlink_file` (`fd` seams, == `AllocateFile("w")`+`FreeFile` / `unlink`),
//!   `kill(PostmasterPid, SIGUSR1)` (the OS boundary, like signalfuncs.c's
//!   `pg_reload_conf`), and `ResetLatch`/`WaitLatch(MyLatch, …)` (the ported
//!   `latch` crate).
//!
//! This crate owns no inward seam crate (no cyclic caller needs it), so it
//! declares no seams and has no `init_seams()`.

// Fallible functions return the shared `types_error::PgResult`.
#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgVec};
use backend_utils_error::ereport;
use types_error::{
    PgError, PgResult, ERROR, WARNING, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SYSTEM_ERROR,
};
use types_core::{TimeLineID, TimestampTz, XLogRecPtr};
use types_wal::{RecoveryPauseState, SessionBackupState, WalLevel, MAXFNAMELEN};

use backend_access_transam_xlog as xlog;
use backend_access_transam_xlog_seams as xlog_seams;
use backend_access_transam_xlogrecovery_seams as xlogrecovery_seams;

/// `XLogIsNeeded()` (xlog.h): `wal_level >= WAL_LEVEL_REPLICA`. The `wal_level`
/// GUC is owned by xlog.c; read it through the xlog-seams accessor and apply the
/// same comparison the xlog crate's `XLogIsNeeded` method does.
fn xlog_is_needed() -> bool {
    xlog_seams::wal_level::call() >= WalLevel::Replica
}

/// `ErrorLocation` for this TU's `ereport` calls (file/line/func, line 0 since
/// the value model does not carry C line numbers).
fn here(funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(
        "../src/backend/access/transam/xlogfuncs.c",
        0,
        funcname,
    )
}

/// The shared "recovery is in progress" / "WAL control functions cannot be
/// executed during recovery." error of the WAL-control functions
/// (`pg_switch_wal`, `pg_current_wal_*`, `pg_walfile_name*`,
/// `pg_create_restore_point`).
fn recovery_in_progress_error() -> PgError {
    PgError::error("recovery is in progress")
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("WAL control functions cannot be executed during recovery.")
}

/// The shared "recovery is in progress" error whose hint names the calling
/// function (`pg_walfile_name_offset()` / `pg_walfile_name()` /
/// `pg_log_standby_snapshot()` use the `"%s cannot be executed during
/// recovery."` form).
fn recovery_in_progress_named_error(funcname: &str) -> PgError {
    PgError::error("recovery is in progress")
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint(format!("{funcname} cannot be executed during recovery."))
}

/// The shared "recovery is not in progress" / "Recovery control functions can
/// only be executed during recovery." error of the recovery-control functions.
fn recovery_not_in_progress_error() -> PgError {
    PgError::error("recovery is not in progress")
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("Recovery control functions can only be executed during recovery.")
}

// ---------------------------------------------------------------------------
// Composite-row result carriers (one field per output column).
// ---------------------------------------------------------------------------

/// The `pg_backup_stop()` result row (`PG_BACKUP_STOP_V2_COLS == 3`).
#[derive(Clone, Debug)]
pub struct BackupStopResult<'mcx> {
    /// `lsn` — the backup stop WAL location (`LSNGetDatum(state->stoppoint)`).
    pub lsn: XLogRecPtr,
    /// `labelfile` — the `backup_label` file contents
    /// (`CStringGetTextDatum(backup_label)`), a `text` payload (verbatim bytes,
    /// server encoding).
    pub labelfile: PgVec<'mcx, u8>,
    /// `spcmapfile` — the tablespace-map file contents
    /// (`CStringGetTextDatum(tablespace_map->data)`), a `text` payload.
    pub spcmapfile: PgVec<'mcx, u8>,
}

/// The `pg_walfile_name_offset()` result row (2 columns).
#[derive(Clone, Debug)]
pub struct WalfileNameOffset<'mcx> {
    /// `file_name` (`text`) — the WAL segment file name.
    pub file_name: PgVec<'mcx, u8>,
    /// `file_offset` (`int4`) — the byte offset within the segment.
    pub file_offset: u32,
}

/// The `pg_split_walfile_name()` result row (`PG_SPLIT_WALFILE_NAME_COLS == 2`).
#[derive(Clone, Debug)]
pub struct SplitWalfileName<'mcx> {
    /// `segment_number` (`numeric`) — the WAL segment sequence number
    /// (`numeric_in(UINT64_FORMAT segno)`), the on-disk varlena byte image.
    pub segment_number: PgVec<'mcx, u8>,
    /// `timeline_id` (`int8`) — the timeline ID.
    pub timeline_id: i64,
}

// ---------------------------------------------------------------------------
// text helper: CStringGetTextDatum over verbatim server-encoding bytes.
// ---------------------------------------------------------------------------

/// `CStringGetTextDatum(s)` (`builtins.h`): wrap NUL-terminated server-encoding
/// bytes into a `text` varlena allocated in `mcx`. `s` here is the cstring's
/// bytes *without* the trailing NUL.
fn cstring_get_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_varlena::keystone::cstring_to_text(mcx, s)
}

// ---------------------------------------------------------------------------
// pg_backup_start / pg_backup_stop
// ---------------------------------------------------------------------------

/// `pg_backup_start(label text, fast bool)` (xlogfuncs.c:57) — set up for taking
/// an on-line backup dump; returns the backup start LSN.
///
/// The C allocates `backup_state`/`tablespace_map` in a dedicated long-lived
/// memory context kept alive until `pg_backup_stop()`. In the value model that
/// long-lived state is the returned `(BackupState, tablespace_map bytes)` pair,
/// which the caller threads to [`pg_backup_stop`]; this function returns the
/// pair alongside the start LSN so the boundary owns the longevity.
pub fn pg_backup_start<'mcx>(
    mcx: Mcx<'mcx>,
    backupid: &str,
    fast: bool,
) -> PgResult<(XLogRecPtr, types_wal::BackupState, PgVec<'mcx, u8>)> {
    let status = xlog_seams::get_backup_status::call();

    // text_to_cstring(backupid): in the value model the label is already decoded.
    let backupidstr = backupid;

    if status == SessionBackupState::Running {
        return Err(PgError::error("a backup is already in progress in this session")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    // The C `backupcontext`/`MemoryContextReset` housekeeping is a no-op in the
    // value model (the state's longevity is the returned tuple, owned by mcx).

    register_persistent_abort_backup_handler()?;
    // do_pg_backup_start(backupidstr, fast, NULL, backup_state, tablespace_map):
    // tablespaces == NULL at this caller, so the out-list is dropped.
    let (backup_state, tablespace_map_bytes) =
        xlog_seams::do_pg_backup_start::call(backupidstr, fast)?;

    let startpoint = backup_state.startpoint();
    let map = mcx::slice_in(mcx, &tablespace_map_bytes)?;

    Ok((startpoint, backup_state, map))
}

/// `register_persistent_abort_backup_handler()` (xlog.c) — register the
/// before_shmem_exit cleanup. Thin wrapper over the seam.
fn register_persistent_abort_backup_handler() -> PgResult<()> {
    xlog_seams::register_persistent_abort_backup_handler::call()
}

/// `pg_backup_stop(waitforarchive bool DEFAULT true)` (xlogfuncs.c:124) — finish
/// an on-line backup; returns `(lsn, labelfile, spcmapfile)`.
///
/// `backup_state` + `tablespace_map` are the values returned by
/// [`pg_backup_start`] (the C file-scope statics). The C asserts they are
/// non-NULL after a `SESSION_BACKUP_RUNNING` status check; here the caller
/// supplies them, so the status check is the in-progress guard and the
/// supplied state is the formerly-static state.
pub fn pg_backup_stop<'mcx>(
    mcx: Mcx<'mcx>,
    waitforarchive: bool,
    backup_state: types_wal::BackupState,
    tablespace_map: &[u8],
) -> PgResult<BackupStopResult<'mcx>> {
    let status = xlog_seams::get_backup_status::call();

    // get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE check:
    // elided (no fcinfo at this boundary; the 3-column row is driven by the
    // caller, cf. fmgr_sql.rs).

    if status != SessionBackupState::Running {
        return Err(PgError::error("backup is not in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Did you call pg_backup_start()?"));
    }

    // Stop the backup; the workhorse fills the stop fields of state.
    let stopped_state = xlog_seams::do_pg_backup_stop::call(backup_state, waitforarchive)?;

    // Build the contents of backup_label: build_backup_content(state, false).
    let backup_label =
        backend_access_transam_xlogbackup::build_backup_content_default(&stopped_state, false)?;

    let lsn = stopped_state.stoppoint();
    let labelfile = cstring_get_text_datum(mcx, &backup_label)?;
    // tablespace_map->data is a NUL-terminated cstring; the carried bytes are
    // already the cstring body (no header), so wrap them directly.
    let spcmapfile = cstring_get_text_datum(mcx, tablespace_map)?;

    // The C frees backup_label, clears the statics and deletes backupcontext:
    // all no-ops in the value model (mcx owns the allocations).

    Ok(BackupStopResult {
        lsn,
        labelfile,
        spcmapfile,
    })
}

// ---------------------------------------------------------------------------
// pg_switch_wal / pg_log_standby_snapshot / pg_create_restore_point
// ---------------------------------------------------------------------------

/// `pg_switch_wal()` (xlogfuncs.c:177) — switch to the next xlog file; returns
/// the WAL location of the switch record.
pub fn pg_switch_wal() -> PgResult<XLogRecPtr> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    let switchpoint = xlog::RequestXLogSwitch(false);

    Ok(switchpoint)
}

/// `pg_log_standby_snapshot()` (xlogfuncs.c:202) — call `LogStandbySnapshot()`;
/// returns the WAL location of the last inserted record.
pub fn pg_log_standby_snapshot() -> PgResult<XLogRecPtr> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_named_error("pg_log_standby_snapshot()"));
    }

    if !xlog_seams::xlog_standby_info_active::call() {
        return Err(PgError::error(
            "pg_log_standby_snapshot() can only be used if \"wal_level\" >= \"replica\"",
        )
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    let recptr = xlog_seams::log_standby_snapshot::call()?;

    Ok(recptr)
}

/// `pg_create_restore_point(name text)` (xlogfuncs.c:233) — a named point for
/// restore; returns the WAL location of the restore-point record.
pub fn pg_create_restore_point(restore_name: &str) -> PgResult<XLogRecPtr> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    if !xlog_is_needed() {
        return Err(PgError::error("WAL level not sufficient for creating a restore point")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(
                "\"wal_level\" must be set to \"replica\" or \"logical\" at server start.",
            ));
    }

    // text_to_cstring(restore_name): already decoded.
    let restore_name_str = restore_name;

    if restore_name_str.len() >= MAXFNAMELEN {
        return Err(PgError::error(format!(
            "value too long for restore point (maximum {} characters)",
            MAXFNAMELEN - 1
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let restorepoint = xlog::XLogRestorePoint(restore_name_str);

    Ok(restorepoint)
}

// ---------------------------------------------------------------------------
// pg_current_wal_lsn / pg_current_wal_insert_lsn / pg_current_wal_flush_lsn
// ---------------------------------------------------------------------------

/// `pg_current_wal_lsn()` (xlogfuncs.c:274) — the current WAL write location.
pub fn pg_current_wal_lsn() -> PgResult<XLogRecPtr> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }
    Ok(xlog::GetXLogWriteRecPtr())
}

/// `pg_current_wal_insert_lsn()` (xlogfuncs.c:295) — the current WAL insert
/// location.
pub fn pg_current_wal_insert_lsn() -> PgResult<XLogRecPtr> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }
    Ok(xlog::GetXLogInsertRecPtr())
}

/// `pg_current_wal_flush_lsn()` (xlogfuncs.c:316) — the current WAL flush
/// location (`GetFlushRecPtr(NULL)`).
pub fn pg_current_wal_flush_lsn() -> PgResult<XLogRecPtr> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }
    // GetFlushRecPtr(NULL): the C passes a NULL insertTLI out-param; the ported
    // fn returns (recptr, tli) and we discard the tli.
    let (current_recptr, _tli) = xlog::shmem::GetFlushRecPtr();
    Ok(current_recptr)
}

// ---------------------------------------------------------------------------
// pg_last_wal_receive_lsn / pg_last_wal_replay_lsn
// ---------------------------------------------------------------------------

/// `pg_last_wal_receive_lsn()` (xlogfuncs.c:338) — the last WAL receive
/// location; `NULL` when none received (`recptr == 0`).
pub fn pg_last_wal_receive_lsn() -> Option<XLogRecPtr> {
    // GetWalRcvFlushRecPtr(NULL, NULL).
    let recptr = backend_replication_walreceiverfuncs::GetWalRcvFlushRecPtr(None, None);
    if recptr == 0 {
        None
    } else {
        Some(recptr)
    }
}

/// `pg_last_wal_replay_lsn()` (xlogfuncs.c:357) — the last WAL replay location;
/// `NULL` when none replayed (`recptr == 0`).
pub fn pg_last_wal_replay_lsn() -> Option<XLogRecPtr> {
    // GetXLogReplayRecPtr(NULL).
    let recptr = xlogrecovery_seams::get_xlog_replay_recptr::call();
    if recptr == 0 {
        None
    } else {
        Some(recptr)
    }
}

// ---------------------------------------------------------------------------
// pg_walfile_name_offset / pg_walfile_name / pg_split_walfile_name
// ---------------------------------------------------------------------------

/// `pg_walfile_name_offset(lsn pg_lsn)` (xlogfuncs.c:374) — compute the WAL
/// file name and decimal byte offset for `locationpoint`.
pub fn pg_walfile_name_offset<'mcx>(
    mcx: Mcx<'mcx>,
    locationpoint: XLogRecPtr,
) -> PgResult<WalfileNameOffset<'mcx>> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_named_error("pg_walfile_name_offset()"));
    }

    // The C builds + blesses a 2-column tuple descriptor (file_name text,
    // file_offset int4); the value model returns the columns directly.

    let wal_segsz = xlog_seams::wal_segment_size::call();

    // xlogfilename = XLogFileName(GetWALInsertionTimeLine(), XLByteToSeg(loc)).
    let xlogsegno = xlog::XLByteToSeg(locationpoint, wal_segsz);
    let xlogfilename = xlog::XLogFileName(xlog::GetWALInsertionTimeLine(), xlogsegno, wal_segsz);
    let file_name = cstring_get_text_datum(mcx, xlogfilename.as_bytes())?;

    // offset = XLogSegmentOffset(locationpoint, wal_segment_size).
    let file_offset = xlog::XLogSegmentOffset(locationpoint, wal_segsz);

    Ok(WalfileNameOffset {
        file_name,
        file_offset,
    })
}

/// `pg_walfile_name(lsn pg_lsn)` (xlogfuncs.c:438) — compute the WAL file name
/// for `locationpoint` (a `text` result).
pub fn pg_walfile_name<'mcx>(
    mcx: Mcx<'mcx>,
    locationpoint: XLogRecPtr,
) -> PgResult<PgVec<'mcx, u8>> {
    if xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_named_error("pg_walfile_name()"));
    }

    let wal_segsz = xlog_seams::wal_segment_size::call();
    let xlogsegno = xlog::XLByteToSeg(locationpoint, wal_segsz);
    let xlogfilename = xlog::XLogFileName(xlog::GetWALInsertionTimeLine(), xlogsegno, wal_segsz);

    // PG_RETURN_TEXT_P(cstring_to_text(xlogfilename)).
    backend_utils_adt_varlena::keystone::cstring_to_text(mcx, xlogfilename.as_bytes())
}

/// `pg_split_walfile_name(file_name text)` (xlogfuncs.c:463) — extract the
/// segment sequence number and timeline ID from a WAL file name.
pub fn pg_split_walfile_name<'mcx>(
    mcx: Mcx<'mcx>,
    fname: &str,
) -> PgResult<SplitWalfileName<'mcx>> {
    // fname = text_to_cstring(PG_GETARG_TEXT_PP(0)): already decoded.

    // fname_upper = pstrdup(fname); capitalize via pg_toupper. WAL file names are
    // pure ASCII hex, so ASCII uppercasing is exact (pg_toupper on a
    // single-byte server encoding uppercases only ASCII here).
    let fname_upper: String = fname
        .bytes()
        .map(|b| b.to_ascii_uppercase() as char)
        .collect();

    if !xlog::IsXLogFileName(&fname_upper) {
        return Err(PgError::error(format!("invalid WAL file name \"{fname}\""))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let wal_segsz = xlog_seams::wal_segment_size::call();
    // XLogFromFileName(fname_upper, &tli, &segno, wal_segment_size).
    let (tli, segno): (TimeLineID, types_core::XLogSegNo) =
        xlog::XLogFromFileName(&fname_upper, wal_segsz)?;

    // get_call_result_type composite-check: elided (value model).

    // values[0] = numeric_in(UINT64_FORMAT segno, 0, -1): the segment number as
    // a numeric. snprintf(buf, UINT64_FORMAT, segno) -> decimal string.
    let buf = format!("{segno}");
    let segment_number = backend_utils_adt_numeric::io::numeric_in(mcx, &buf, -1)?;

    // values[1] = Int64GetDatum(tli).
    let timeline_id = tli as i64;

    Ok(SplitWalfileName {
        segment_number,
        timeline_id,
    })
}

// ---------------------------------------------------------------------------
// Recovery pause / resume / state
// ---------------------------------------------------------------------------

/// `pg_wal_replay_pause()` (xlogfuncs.c:518) — request to pause recovery.
pub fn pg_wal_replay_pause() -> PgResult<()> {
    if !xlogrecovery_seams::in_recovery::call() {
        return Err(recovery_not_in_progress_error());
    }

    if xlogrecovery_seams::promote_is_triggered::call() {
        return Err(promotion_ongoing_error("pg_wal_replay_pause()"));
    }

    xlogrecovery_seams::set_recovery_pause::call(true);

    // wake up the recovery process so it can process the pause request.
    xlogrecovery_seams::wakeup_recovery::call();

    Ok(())
}

/// `pg_wal_replay_resume()` (xlogfuncs.c:548) — resume recovery now.
pub fn pg_wal_replay_resume() -> PgResult<()> {
    if !xlogrecovery_seams::in_recovery::call() {
        return Err(recovery_not_in_progress_error());
    }

    if xlogrecovery_seams::promote_is_triggered::call() {
        return Err(promotion_ongoing_error("pg_wal_replay_resume()"));
    }

    xlogrecovery_seams::set_recovery_pause::call(false);

    Ok(())
}

/// The "standby promotion is ongoing" error of `pg_wal_replay_pause/resume`.
fn promotion_ongoing_error(funcname: &str) -> PgError {
    PgError::error("standby promotion is ongoing")
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint(format!(
            "{funcname} cannot be executed after promotion is triggered."
        ))
}

/// `pg_is_wal_replay_paused()` (xlogfuncs.c:572) — whether recovery is paused
/// (`GetRecoveryPauseState() != RECOVERY_NOT_PAUSED`).
pub fn pg_is_wal_replay_paused() -> PgResult<bool> {
    if !xlogrecovery_seams::in_recovery::call() {
        return Err(recovery_not_in_progress_error());
    }

    Ok(xlogrecovery_seams::get_recovery_pause_state::call() != RecoveryPauseState::NotPaused)
}

/// `pg_get_wal_replay_pause_state()` (xlogfuncs.c:593) — the recovery pause
/// state as a `text` label ("not paused" / "pause requested" / "paused").
pub fn pg_get_wal_replay_pause_state<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    if !xlogrecovery_seams::in_recovery::call() {
        return Err(recovery_not_in_progress_error());
    }

    let statestr: &str = match xlogrecovery_seams::get_recovery_pause_state::call() {
        RecoveryPauseState::NotPaused => "not paused",
        RecoveryPauseState::PauseRequested => "pause requested",
        RecoveryPauseState::Paused => "paused",
    };

    // PG_RETURN_TEXT_P(cstring_to_text(statestr)).
    backend_utils_adt_varlena::keystone::cstring_to_text(mcx, statestr.as_bytes())
}

// ---------------------------------------------------------------------------
// pg_last_xact_replay_timestamp / pg_is_in_recovery / pg_wal_lsn_diff
// ---------------------------------------------------------------------------

/// `pg_last_xact_replay_timestamp()` (xlogfuncs.c:628) — timestamp of the
/// latest processed commit/abort record; `NULL` when started normally without
/// recovery (`xtime == 0`).
pub fn pg_last_xact_replay_timestamp() -> Option<TimestampTz> {
    let xtime = xlogrecovery_seams::get_latest_x_time::call();
    if xtime == 0 {
        None
    } else {
        Some(xtime)
    }
}

/// `pg_is_in_recovery()` (xlogfuncs.c:643) — the current recovery mode.
pub fn pg_is_in_recovery() -> bool {
    xlog::RecoveryInProgress()
}

/// `pg_wal_lsn_diff(lsn1 pg_lsn, lsn2 pg_lsn)` (xlogfuncs.c:652) — the byte
/// difference between two WAL locations (a `numeric`).
///
/// C: `DirectFunctionCall2(pg_lsn_mi, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1))`.
pub fn pg_wal_lsn_diff<'mcx>(
    mcx: Mcx<'mcx>,
    lsn1: XLogRecPtr,
    lsn2: XLogRecPtr,
) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_lsn_trigfuncs::pg_lsn::pg_lsn_mi(mcx, lsn1, lsn2)
}

// ---------------------------------------------------------------------------
// pg_promote
// ---------------------------------------------------------------------------

/// `PROMOTE_SIGNAL_FILE` (access/xlog.h:314).
const PROMOTE_SIGNAL_FILE: &str = "promote";

/// `WAITS_PER_SECOND` (xlogfuncs.c:716).
const WAITS_PER_SECOND: i32 = 10;

/// `pg_promote(wait bool DEFAULT true, wait_seconds int DEFAULT 60)`
/// (xlogfuncs.c:670) — promote a standby server.
///
/// Returns `true` when promotion has completed (if `wait`) or been initiated
/// (if `!wait`); `false` when the wait timed out.
pub fn pg_promote(wait: bool, wait_seconds: i32) -> PgResult<bool> {
    if !xlogrecovery_seams::in_recovery::call() {
        return Err(recovery_not_in_progress_error());
    }

    if wait_seconds <= 0 {
        return Err(PgError::error("\"wait_seconds\" must not be negative or zero")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }

    // create the promote signal file: AllocateFile(PROMOTE_SIGNAL_FILE, "w")
    // then FreeFile. The fd seam's `create_empty_file` is exactly this
    // open("w")+close pair, returning the C's two distinct failure modes.
    match backend_storage_file_fd_seams::create_empty_file::call(PROMOTE_SIGNAL_FILE) {
        backend_storage_file_fd_seams::CreateEmptyFileOutcome::Ok => {}
        backend_storage_file_fd_seams::CreateEmptyFileOutcome::CreateFailed(errno) => {
            return ereport(ERROR)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not create file \"{PROMOTE_SIGNAL_FILE}\": %m"))
                .finish(here("pg_promote"))
                .map(|()| unreachable!());
        }
        backend_storage_file_fd_seams::CreateEmptyFileOutcome::WriteFailed(errno) => {
            return ereport(ERROR)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not write file \"{PROMOTE_SIGNAL_FILE}\": %m"))
                .finish(here("pg_promote"))
                .map(|()| unreachable!());
        }
    }

    // signal the postmaster: kill(PostmasterPid, SIGUSR1). This is the OS
    // boundary, exactly as signalfuncs.c's pg_reload_conf does (read
    // PostmasterPid via globals seam, raw kill).
    let postmaster_pid = backend_utils_init_small_seams::postmaster_pid::call();
    if unsafe { libc::kill(postmaster_pid, libc::SIGUSR1) } != 0 {
        // (void) unlink(PROMOTE_SIGNAL_FILE): return value ignored, like the C.
        let _rc = backend_storage_file_fd_seams::unlink_file::call(PROMOTE_SIGNAL_FILE);
        return Err(PgError::error("failed to send signal to postmaster: %m")
            .with_sqlstate(ERRCODE_SYSTEM_ERROR));
    }

    // return immediately if waiting was not requested.
    if !wait {
        return Ok(true);
    }

    // wait for the amount of time wanted until promotion.
    for _i in 0..(WAITS_PER_SECOND * wait_seconds) {
        backend_storage_ipc_latch::ResetLatch(my_latch());

        if !xlogrecovery_seams::in_recovery::call() {
            return Ok(true);
        }

        backend_tcop_postgres_seams::check_for_interrupts::call()?;

        let rc = backend_storage_ipc_latch::WaitLatch(
            Some(my_latch()),
            types_storage::waiteventset::WL_LATCH_SET
                | types_storage::waiteventset::WL_TIMEOUT
                | types_storage::waiteventset::WL_POSTMASTER_DEATH,
            (1000 / WAITS_PER_SECOND) as i64,
            types_pgstat::wait_event::WAIT_EVENT_PROMOTE,
        )?;

        // Emergency bailout if postmaster has died.
        if rc & types_storage::waiteventset::WL_POSTMASTER_DEATH != 0 {
            // C: ereport(FATAL, ...). The error builder carries FATAL on Err.
            return ereport(types_error::FATAL)
                .errcode(types_error::ERRCODE_ADMIN_SHUTDOWN)
                .errmsg("terminating connection due to unexpected postmaster exit")
                .errcontext_msg("while waiting on promotion")
                .finish(here("pg_promote"))
                .map(|()| unreachable!());
        }
    }

    // server did not promote within N second(s): a WARNING (non-throwing).
    ereport(WARNING)
        .errmsg_plural(
            format!("server did not promote within {wait_seconds} second"),
            format!("server did not promote within {wait_seconds} seconds"),
            wait_seconds as u64,
        )
        .finish(here("pg_promote"))?;

    Ok(false)
}

/// `MyLatch` (globals.c): this backend's own process latch, read via the ported
/// `latch` crate (it owns `MyLatch` until miscinit/globals land).
fn my_latch() -> types_storage::latch::LatchHandle {
    backend_storage_ipc_latch::my_latch().expect("MyLatch is NULL in pg_promote")
}

#[cfg(test)]
mod tests;
