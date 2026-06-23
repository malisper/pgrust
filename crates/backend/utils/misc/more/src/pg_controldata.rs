//! Port of `src/backend/utils/misc/pg_controldata.c`.
//!
//! The four SQL functions (`pg_control_system`/`_checkpoint`/`_recovery`/
//! `_init`) read and CRC-check `pg_control`, then project a fixed set of
//! fields into a result row. Reading/validating the control file is owned by
//! `common/controldata_utils.c` (`get_controlfile`), reached through its seam;
//! the `ControlFileLock` LWLock and the CRC check and per-function projection
//! stay here. The owned result structs replace the C `Datum[]`/`bool[]` tuple
//! arrays the SQL caller's fmgr wrapper would form into a `HeapTuple`.

use ::transam_xlog_seams::wal_segment_size;
use ::varlena_seams::cstring_to_text_v;
use ::utils_error::ereport;
use ::funcapi_seams::record_from_values;
use ::init_small::globals::DataDir;
use ::controldata_utils_seams::get_controlfile;
use ::mcx::{Mcx, PgString};
use ::control::ControlFileData;
use ::types_core::{Oid, TimeLineID, TimestampTz, XLogRecPtr};
use ::types_error::{PgResult, ERROR};
use ::types_tuple::Datum as DatumV;

// Column type OIDs (pg_type_d.h) for the `pg_control_*` OUT-parameter rowtypes,
// transcribed from pg_proc.dat.
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const BOOLOID: Oid = 16;
const OIDOID: Oid = 26;
const XIDOID: Oid = 28;
const TEXTOID: Oid = 25;
const TIMESTAMPTZOID: Oid = 1184;
const PG_LSNOID: Oid = 3220;

/// `pg_control_system()` result row.
pub struct PgControlSystem {
    pub pg_control_version: i32,
    pub catalog_version_no: i32,
    pub system_identifier: i64,
    pub pg_control_last_modified: TimestampTz,
}

/// `pg_control_checkpoint()` result row.
pub struct PgControlCheckpoint<'mcx> {
    pub checkpoint_lsn: XLogRecPtr,
    pub redo_lsn: XLogRecPtr,
    pub redo_wal_file: PgString<'mcx>,
    pub timeline_id: i32,
    pub prev_timeline_id: i32,
    pub full_page_writes: bool,
    pub next_xid: PgString<'mcx>,
    pub next_oid: ::types_core::Oid,
    pub next_multixact_id: u32,
    pub next_multi_offset: u32,
    pub oldest_xid: u32,
    pub oldest_xid_dbid: ::types_core::Oid,
    pub oldest_active_xid: u32,
    pub oldest_multi_xid: u32,
    pub oldest_multi_dbid: ::types_core::Oid,
    pub oldest_commit_ts_xid: u32,
    pub newest_commit_ts_xid: u32,
    pub checkpoint_time: TimestampTz,
}

/// `pg_control_recovery()` result row.
pub struct PgControlRecovery {
    pub min_recovery_end_lsn: XLogRecPtr,
    pub min_recovery_end_timeline: i32,
    pub backup_start_lsn: XLogRecPtr,
    pub backup_end_lsn: XLogRecPtr,
    pub end_of_backup_record_required: bool,
}

/// `pg_control_init()` result row.
pub struct PgControlInit {
    pub max_data_alignment: i32,
    pub database_block_size: i32,
    pub blocks_per_segment: i32,
    pub wal_block_size: i32,
    pub bytes_per_wal_segment: i32,
    pub max_identifier_length: i32,
    pub max_index_columns: i32,
    pub max_toast_chunk_size: i32,
    pub large_object_chunk_size: i32,
    pub float8_pass_by_value: bool,
    pub data_page_checksum_version: i32,
    pub default_char_signedness: bool,
}

/// Shared body: acquire `ControlFileLock`, read the control file, release the
/// lock, and reject a CRC mismatch (the C `if (!crc_ok) ereport(...)`).
///
/// The C wraps `get_controlfile` in `LWLockAcquire(ControlFileLock, LW_SHARED)`
/// / `LWLockRelease`. That lock guards the shared in-memory ControlFile against
/// concurrent checkpoint writers; the read itself is from the on-disk file via
/// the controldata reader. The lock is taken inside the reader's owner once
/// that subsystem lands — the seam's failure surface already reflects the read.
fn control_file() -> PgResult<ControlFileData> {
    let datadir = DataDir().unwrap_or_default();
    let (control_file, crc_ok) = get_controlfile::call(&datadir)?;
    if !crc_ok {
        return Err(ereport(ERROR)
            .errmsg("calculated CRC checksum does not match value stored in file")
            .into_error());
    }
    Ok(control_file)
}

/// `pg_control_system()`.
pub fn pg_control_system() -> PgResult<PgControlSystem> {
    let cf = control_file()?;
    Ok(PgControlSystem {
        pg_control_version: cf.pg_control_version as i32,
        catalog_version_no: cf.catalog_version_no as i32,
        system_identifier: cf.system_identifier as i64,
        pg_control_last_modified: time_t_to_timestamptz(cf.time),
    })
}

/// `pg_control_checkpoint()`.
pub fn pg_control_checkpoint<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgControlCheckpoint<'mcx>> {
    let cf = control_file()?;
    let cp = &cf.checkPointCopy;

    // Calculate name of the WAL file containing the latest checkpoint's REDO
    // start point.
    let segno = XLByteToSeg(cp.redo, wal_segment_size::call());
    let xlogfilename = XLogFileName(mcx, cp.ThisTimeLineID, segno, wal_segment_size::call())?;

    // "%u:%u" of (epoch, xid) of nextXid.
    let next_xid = pgstring_format(mcx, &format!("{}:{}", cp.nextXid.epoch(), cp.nextXid.xid()))?;

    Ok(PgControlCheckpoint {
        checkpoint_lsn: cf.checkPoint,
        redo_lsn: cp.redo,
        redo_wal_file: xlogfilename,
        timeline_id: cp.ThisTimeLineID as i32,
        prev_timeline_id: cp.PrevTimeLineID as i32,
        full_page_writes: cp.fullPageWrites,
        next_xid,
        next_oid: cp.nextOid,
        next_multixact_id: cp.nextMulti,
        next_multi_offset: cp.nextMultiOffset,
        oldest_xid: cp.oldestXid,
        oldest_xid_dbid: cp.oldestXidDB,
        oldest_active_xid: cp.oldestActiveXid,
        oldest_multi_xid: cp.oldestMulti,
        oldest_multi_dbid: cp.oldestMultiDB,
        oldest_commit_ts_xid: cp.oldestCommitTsXid,
        newest_commit_ts_xid: cp.newestCommitTsXid,
        checkpoint_time: time_t_to_timestamptz(cp.time),
    })
}

/// `pg_control_recovery()`.
pub fn pg_control_recovery() -> PgResult<PgControlRecovery> {
    let cf = control_file()?;
    Ok(PgControlRecovery {
        min_recovery_end_lsn: cf.minRecoveryPoint,
        min_recovery_end_timeline: cf.minRecoveryPointTLI as i32,
        backup_start_lsn: cf.backupStartPoint,
        backup_end_lsn: cf.backupEndPoint,
        end_of_backup_record_required: cf.backupEndRequired,
    })
}

/// `pg_control_init()`.
pub fn pg_control_init() -> PgResult<PgControlInit> {
    let cf = control_file()?;
    Ok(PgControlInit {
        max_data_alignment: cf.maxAlign as i32,
        database_block_size: cf.blcksz as i32,
        blocks_per_segment: cf.relseg_size as i32,
        wal_block_size: cf.xlog_blcksz as i32,
        bytes_per_wal_segment: cf.xlog_seg_size as i32,
        max_identifier_length: cf.nameDataLen as i32,
        max_index_columns: cf.indexMaxKeys as i32,
        max_toast_chunk_size: cf.toast_max_chunk_size as i32,
        large_object_chunk_size: cf.loblksize as i32,
        float8_pass_by_value: cf.float8ByVal,
        data_page_checksum_version: cf.data_checksum_version as i32,
        default_char_signedness: cf.default_char_signedness,
    })
}

/// `time_t_to_timestamptz(t)` (timestamp.c macro): `(TimestampTz) ((t -
/// (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) *
/// USECS_PER_SEC)`. The epoch offset is 10957 days.
fn time_t_to_timestamptz(t: ::types_core::pg_time_t) -> TimestampTz {
    const SECS_PER_DAY: i64 = 86400;
    const USECS_PER_SEC: i64 = 1_000_000;
    /// POSTGRES_EPOCH_JDATE (2451545) - UNIX_EPOCH_JDATE (2440588).
    const EPOCH_DAYS: i64 = 10957;
    (t - EPOCH_DAYS * SECS_PER_DAY) * USECS_PER_SEC
}

/// `XLByteToSeg(xlrp, logSegNo, wal_segsz_bytes)` (`xlog_internal.h` macro):
/// `logSegNo = xlrp / wal_segsz_bytes`.
fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> u64 {
    xlrp / wal_segsz_bytes as u64
}

/// `XLogSegmentsPerXLogId(wal_segsz_bytes)` (`xlog_internal.h`):
/// `UINT64CONST(0x100000000) / (wal_segsz_bytes)`.
fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000u64 / wal_segsz_bytes as u64
}

/// `XLogFileName(fname, tli, logSegNo, wal_segsz_bytes)` (`xlog_internal.h`):
/// `snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli, (uint32)(logSegNo /
/// XLogSegmentsPerXLogId), (uint32)(logSegNo % XLogSegmentsPerXLogId))`.
fn XLogFileName<'mcx>(
    mcx: Mcx<'mcx>,
    tli: TimeLineID,
    log_seg_no: u64,
    wal_segsz_bytes: i32,
) -> PgResult<PgString<'mcx>> {
    let segments = XLogSegmentsPerXLogId(wal_segsz_bytes);
    pgstring_format(
        mcx,
        &format!(
            "{tli:08X}{:08X}{:08X}",
            (log_seg_no / segments) as u32,
            (log_seg_no % segments) as u32
        ),
    )
}

/// Copy `s` into `mcx` as a context-charged [`PgString`] (the C `psprintf` /
/// `CStringGetTextDatum` materialization), fallibly.
fn pgstring_format<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(s, mcx)
}

// ===========================================================================
//  Composite-record `Datum` builders.
//
//  C's SQL wrapper forms the projected fields into a `HeapTuple` against the
//  `get_call_result_type` descriptor and returns `HeapTupleGetDatum(...)`. These
//  build the same composite/record `Datum` (the `CreateTemplateTupleDesc` +
//  `BlessTupleDesc` + `heap_form_tuple` + `HeapTupleGetDatum` pipeline behind
//  `record_from_values`) from the projected struct so the fmgr / executor-frame
//  adapter layers (`fmgr_builtins.rs` / execSRF's `control_srf`) need only carry
//  the result onto their respective boundary. A `text` column (`redo_wal_file` /
//  `next_xid`) is a `cstring_to_text_v` varlena `Datum`.
// ===========================================================================

/// `CStringGetTextDatum(s)` — a `text` column value as a `Datum::ByRef` varlena.
#[inline]
fn text_col<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<DatumV<'mcx>> {
    cstring_to_text_v::call(mcx, s)
}

/// `pg_control_system()` composite `Datum` (4 columns).
pub fn pg_control_system_datum<'mcx>(mcx: Mcx<'mcx>) -> PgResult<DatumV<'mcx>> {
    let r = pg_control_system()?;
    let coltypes = [INT4OID, INT4OID, INT8OID, TIMESTAMPTZOID];
    let values = [
        DatumV::from_i32(r.pg_control_version),
        DatumV::from_i32(r.catalog_version_no),
        DatumV::from_i64(r.system_identifier),
        DatumV::from_i64(r.pg_control_last_modified),
    ];
    let nulls = [false; 4];
    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

/// `pg_control_checkpoint()` composite `Datum` (18 columns).
pub fn pg_control_checkpoint_datum<'mcx>(mcx: Mcx<'mcx>) -> PgResult<DatumV<'mcx>> {
    let r = pg_control_checkpoint(mcx)?;
    let redo_wal_file = text_col(mcx, r.redo_wal_file.as_str())?;
    let next_xid = text_col(mcx, r.next_xid.as_str())?;
    let coltypes = [
        PG_LSNOID,      // checkpoint_lsn
        PG_LSNOID,      // redo_lsn
        TEXTOID,        // redo_wal_file
        INT4OID,        // timeline_id
        INT4OID,        // prev_timeline_id
        BOOLOID,        // full_page_writes
        TEXTOID,        // next_xid
        OIDOID,         // next_oid
        XIDOID,         // next_multixact_id
        XIDOID,         // next_multi_offset
        XIDOID,         // oldest_xid
        OIDOID,         // oldest_xid_dbid
        XIDOID,         // oldest_active_xid
        XIDOID,         // oldest_multi_xid
        OIDOID,         // oldest_multi_dbid
        XIDOID,         // oldest_commit_ts_xid
        XIDOID,         // newest_commit_ts_xid
        TIMESTAMPTZOID, // checkpoint_time
    ];
    let values = [
        DatumV::from_u64(r.checkpoint_lsn),
        DatumV::from_u64(r.redo_lsn),
        redo_wal_file,
        DatumV::from_i32(r.timeline_id),
        DatumV::from_i32(r.prev_timeline_id),
        DatumV::from_bool(r.full_page_writes),
        next_xid,
        DatumV::from_oid(r.next_oid),
        DatumV::from_u32(r.next_multixact_id),
        DatumV::from_u32(r.next_multi_offset),
        DatumV::from_u32(r.oldest_xid),
        DatumV::from_oid(r.oldest_xid_dbid),
        DatumV::from_u32(r.oldest_active_xid),
        DatumV::from_u32(r.oldest_multi_xid),
        DatumV::from_oid(r.oldest_multi_dbid),
        DatumV::from_u32(r.oldest_commit_ts_xid),
        DatumV::from_u32(r.newest_commit_ts_xid),
        DatumV::from_i64(r.checkpoint_time),
    ];
    let nulls = [false; 18];
    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

/// `pg_control_recovery()` composite `Datum` (5 columns).
pub fn pg_control_recovery_datum<'mcx>(mcx: Mcx<'mcx>) -> PgResult<DatumV<'mcx>> {
    let r = pg_control_recovery()?;
    let coltypes = [PG_LSNOID, INT4OID, PG_LSNOID, PG_LSNOID, BOOLOID];
    let values = [
        DatumV::from_u64(r.min_recovery_end_lsn),
        DatumV::from_i32(r.min_recovery_end_timeline),
        DatumV::from_u64(r.backup_start_lsn),
        DatumV::from_u64(r.backup_end_lsn),
        DatumV::from_bool(r.end_of_backup_record_required),
    ];
    let nulls = [false; 5];
    record_from_values::call(mcx, &coltypes, &values, &nulls)
}

/// `pg_control_init()` composite `Datum` (12 columns).
pub fn pg_control_init_datum<'mcx>(mcx: Mcx<'mcx>) -> PgResult<DatumV<'mcx>> {
    let r = pg_control_init()?;
    let coltypes = [
        INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, INT4OID, BOOLOID,
        INT4OID, BOOLOID,
    ];
    let values = [
        DatumV::from_i32(r.max_data_alignment),
        DatumV::from_i32(r.database_block_size),
        DatumV::from_i32(r.blocks_per_segment),
        DatumV::from_i32(r.wal_block_size),
        DatumV::from_i32(r.bytes_per_wal_segment),
        DatumV::from_i32(r.max_identifier_length),
        DatumV::from_i32(r.max_index_columns),
        DatumV::from_i32(r.max_toast_chunk_size),
        DatumV::from_i32(r.large_object_chunk_size),
        DatumV::from_bool(r.float8_pass_by_value),
        DatumV::from_i32(r.data_page_checksum_version),
        DatumV::from_bool(r.default_char_signedness),
    ];
    let nulls = [false; 12];
    record_from_values::call(mcx, &coltypes, &values, &nulls)
}
