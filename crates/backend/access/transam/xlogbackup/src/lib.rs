#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::{pg_time_t, TimeLineID, XLogRecPtr, XLogSegNo};

// tablespaceinfo (basebackup.h) — one auxiliary-tablespace entry produced by
// do_pg_backup_start and consumed by the base-backup driver + sink chain. Homed
// here (shared, below the backup layer) to avoid a transam_xlog -> sink
// inversion. `size` mirrors C's int64 (-1 = unknown).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TablespaceInfo {
    pub oid: types_core::Oid,
    pub path: Option<String>,
    pub rpath: Option<String>,
    pub size: i64,
}
use types_error::PgResult;

const MAXPGPATH: usize = 1024;
const InvalidXLogRecPtr: XLogRecPtr = 0;

// name is C's char[MAXPGPATH + 1]: server-encoding label bytes, NUL-terminated,
// never UTF-8-validated.
pub struct BackupState {
    pub name: [u8; MAXPGPATH + 1],
    pub startpoint: XLogRecPtr,
    pub starttli: TimeLineID,
    pub checkpointloc: XLogRecPtr,
    pub starttime: pg_time_t,
    pub started_in_recovery: bool,
    pub istartpoint: XLogRecPtr,
    pub istarttli: TimeLineID,
    pub stoppoint: XLogRecPtr,
    pub stoptli: TimeLineID,
    pub stoptime: pg_time_t,
}

impl Default for BackupState {
    fn default() -> Self {
        BackupState {
            name: [0; MAXPGPATH + 1],
            startpoint: 0,
            starttli: 0,
            checkpointloc: 0,
            starttime: 0,
            started_in_recovery: false,
            istartpoint: 0,
            istarttli: 0,
            stoppoint: 0,
            stoptli: 0,
            stoptime: 0,
        }
    }
}

impl BackupState {
    pub fn name_bytes(&self) -> &[u8] {
        let end = self.name.iter().position(|&b| b == 0).unwrap_or(self.name.len());
        &self.name[..end]
    }

    pub fn set_name(&mut self, name: &[u8]) {
        debug_assert!(name.len() <= MAXPGPATH);
        self.name = [0; MAXPGPATH + 1];
        self.name[..name.len()].copy_from_slice(name);
    }
}

fn XLogSegmentsPerXLogId(wal_segsz_bytes: i32) -> u64 {
    0x1_0000_0000u64 / wal_segsz_bytes as u64
}

fn XLByteToSeg(xlrp: XLogRecPtr, wal_segsz_bytes: i32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: i32) -> String {
    let per_id = XLogSegmentsPerXLogId(wal_segsz_bytes);
    format!("{tli:08X}{:08X}{:08X}", logSegNo / per_id, logSegNo % per_id)
}

// Log timezone, not session timezone; C NULL-derefs on failure — loud here.
fn format_backup_time(t: pg_time_t) -> ([u8; 128], usize) {
    let tz = pgtz::log_timezone().expect("log_timezone not initialized");
    let tm = localtime::pg_localtime(t, tz).expect("pg_localtime failed for backup time");
    let mut buf = [0u8; 128];
    let len = strftime::pg_strftime(&mut buf, b"%Y-%m-%d %H:%M:%S %Z", &tm)
        .expect("backup time did not fit in 128 bytes");
    (buf, len)
}

pub fn build_backup_content<'mcx>(
    mcx: Mcx<'mcx>,
    state: &BackupState,
    ishistoryfile: bool,
    wal_segment_size: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut result = StringInfo::new_in(mcx)?;

    let (startstrbuf, startstrlen) = format_backup_time(state.starttime);

    let startsegno = XLByteToSeg(state.startpoint, wal_segment_size);
    let startxlogfile = XLogFileName(state.starttli, startsegno, wal_segment_size);
    result.append_str(&format!(
        "START WAL LOCATION: {:X}/{:X} (file {startxlogfile})\n",
        (state.startpoint >> 32) as u32,
        state.startpoint as u32
    ))?;

    if ishistoryfile {
        let stopsegno = XLByteToSeg(state.stoppoint, wal_segment_size);
        let stopxlogfile = XLogFileName(state.stoptli, stopsegno, wal_segment_size);
        result.append_str(&format!(
            "STOP WAL LOCATION: {:X}/{:X} (file {stopxlogfile})\n",
            (state.stoppoint >> 32) as u32,
            state.stoppoint as u32
        ))?;
    }

    result.append_str(&format!(
        "CHECKPOINT LOCATION: {:X}/{:X}\n",
        (state.checkpointloc >> 32) as u32,
        state.checkpointloc as u32
    ))?;
    result.append_str("BACKUP METHOD: streamed\n")?;
    result.append_str(if state.started_in_recovery {
        "BACKUP FROM: standby\n"
    } else {
        "BACKUP FROM: primary\n"
    })?;
    result.append_str("START TIME: ")?;
    result.append_bytes(&startstrbuf[..startstrlen])?;
    result.append_str("\nLABEL: ")?;
    result.append_bytes(state.name_bytes())?;
    result.append_str(&format!("\nSTART TIMELINE: {}\n", state.starttli))?;

    if ishistoryfile {
        let (stopstrfbuf, stopstrlen) = format_backup_time(state.stoptime);
        result.append_str("STOP TIME: ")?;
        result.append_bytes(&stopstrfbuf[..stopstrlen])?;
        result.append_str(&format!("\nSTOP TIMELINE: {}\n", state.stoptli))?;
    }

    // either both istartpoint and istarttli should be set, or neither
    debug_assert!((state.istartpoint == InvalidXLogRecPtr) == (state.istarttli == 0));
    if state.istartpoint != InvalidXLogRecPtr {
        result.append_str(&format!(
            "INCREMENTAL FROM LSN: {:X}/{:X}\n",
            (state.istartpoint >> 32) as u32,
            state.istartpoint as u32
        ))?;
        result.append_str(&format!("INCREMENTAL FROM TLI: {}\n", state.istarttli))?;
    }

    Ok(result.into_vec())
}

#[cfg(test)]
mod tests;
