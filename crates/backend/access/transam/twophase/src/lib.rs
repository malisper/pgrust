//! `backend-access-transam-twophase` — two-phase commit support
//! (`src/backend/access/transam/twophase.c`), idiomatic owned-Rust port.
//!
//! The genuinely-shared `TwoPhaseStateData` / `GXACT` array lives in shared
//! memory protected by `TwoPhaseStateLock`. This crate ports the *algorithm*
//! over an owned [`TwoPhaseStateData`]; the shmem stand-up of that state and
//! the `TwoPhaseStateLock` LWLock that materialize it in production are owned
//! by the lwlock/shmem subsystems and reached through their seams. Every
//! outward touchpoint (dummy-PGPROC, ProcArray, WAL insert/flush, clog /
//! commit-ts / subtrans, the `pg_twophase` file I/O, replication / syncrep /
//! predicate / inval / smgr / pgstat) goes through the owning unit's seam
//! crate and panics loudly until that owner lands.

#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use utils_error::ereport;
use types_error::{ErrorLocation, PgError, PgResult, ERROR, PANIC, WARNING};
use types_core::xact::{InvalidTransactionId, InvalidXLogRecPtr, TransactionIdIsValid};
use types_core::{Oid, ProcNumber, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};
use ::wal::wal::RelFileLocator;
use ::wal::xact_records::{XactLogAbortRecordArgs, XactLogCommitRecordArgs, XlXactOrigin};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Outward seam routing — one `use` alias per owning unit's seam crate.
// ---------------------------------------------------------------------------
use commit_ts_seams as commit_ts;
use transam as transam; // ported: did_commit/abort, commit/abort tree
use subtrans_seams as subtrans;
use varsup_seams as varsup;
use transam_xact_seams as xact;
use transam_xlog_seams as wal; // xlog insert/flush/read + crit section
use xlogreader_seams as xlogreader; // handle-based WAL record reader
use origin_seams as origin;
use syncrep_seams as syncrep; // SyncRepWaitForLSN
use twophase_fileio_seams as files; // pg_twophase file body I/O
use procarray_seams as procarray; // dummy-proc ProcArray add/remove
use dsm_core_seams as ipc; // before_shmem_exit
use ipc_shmem_seams as ipc_shmem; // ShmemInitStruct (2PC shmem stand-up)
use standby_seams as standby;
use lwlock_seams as lwlock; // TwoPhaseStateLock
use predicate_seams as predicate;
use lmgr_proc_seams as proc; // dummy-PGPROC field touch
use inval_seams as inval;
use miscinit_seams as miscinit; // crit section / interrupts / superuser
use twophase_rmgr as rmgrcb; // 2PC rmgr callback tables (direct dep)
use rmgrcb::TwoPhaseCallback;
use stat_seams as pgstat; // transactional drops / AtEOXact_PgStat
use catalog_storage_seams as storage_smgr; // DropRelationFiles
use init_small_seams as init_small; // MyDatabaseId / MyDatabaseTableSpace
use timestamp_seams as timestamp; // GetCurrentTimestamp
use snapmgr_pc_seams as snapmgr; // TransactionXmin

/// Source location stamped onto raised errors (twophase.c).
fn here() -> ErrorLocation {
    ErrorLocation::new("../src/backend/access/transam/twophase.c", 0, "twophase")
}

/// Raise a built `ERROR`-level report as a typed `Err` (the C `ereport(ERROR)`
/// longjmp analog).
fn raise<T>(b: utils_error::ErrorBuilder) -> PgResult<T> {
    Err(b.into_error().with_error_location(here()))
}

// ---------------------------------------------------------------------------
// Constants (twophase.c / xact.h / twophase_rmgr.h)
// ---------------------------------------------------------------------------

/// `GIDSIZE` (xact.h). Max GID length, fitting the uint16 `gidlen`.
pub const GIDSIZE: usize = 200;

/// `TWOPHASE_MAGIC` — the 2PC state-file format identifier.
pub const TWOPHASE_MAGIC: u32 = 0x57F9_4534;

/// `MAXIMUM_ALIGNOF` (8 on supported 64-bit platforms).
pub const MAXIMUM_ALIGNOF: usize = 8;

/// `INVALID_PROC_NUMBER` (procnumber.h).
pub const INVALID_PROC_NUMBER: ProcNumber = -1;

/// `InvalidRepOriginId` (0) / `DoNotReplicateId` (0xFFFF) (origin.h).
pub const INVALID_REP_ORIGIN_ID: RepOriginId = 0;
pub const DO_NOT_REPLICATE_ID: RepOriginId = 0xFFFF;

/// `XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK` (xact.h).
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: i32 = 1 << 1;

/// `MaxAllocSize` (memutils.h) = 0x3fffffff.
pub const MAX_ALLOC_SIZE: u32 = 0x3fff_ffff;

/// `TWOPHASE_DIR` (twophase.c:112) — the subdirectory holding 2PC state files.
pub const TWOPHASE_DIR: &str = "pg_twophase";

/// Two-phase resource-manager ids (twophase_rmgr.h).
pub const TWOPHASE_RM_END_ID: u8 = 0;
pub const TWOPHASE_RM_MAX_ID: u8 = 4;

/// `ProcessRecords` phase selectors for the rmgr-callback seam.
pub const TWOPHASE_PHASE_RECOVER: u8 = 0;
pub const TWOPHASE_PHASE_POSTCOMMIT: u8 = 1;
pub const TWOPHASE_PHASE_POSTABORT: u8 = 2;

/// `MAXALIGN(len)`.
#[inline]
pub const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

#[inline]
fn xlog_rec_ptr_is_invalid(p: XLogRecPtr) -> bool {
    p == InvalidXLogRecPtr
}

// ---- CRC32C (port/pg_crc32c) ----

#[inline]
const fn init_crc32c() -> u32 {
    0xffff_ffff
}
#[inline]
const fn fin_crc32c(crc: u32) -> u32 {
    crc ^ 0xffff_ffff
}
#[inline]
const fn eq_crc32c(c1: u32, c2: u32) -> bool {
    c1 == c2
}
#[inline]
fn comp_crc32c(crc: u32, data: &[u8]) -> u32 {
    crc32c_seams::comp_crc32c::call(crc, data)
}

// ---------------------------------------------------------------------------
// TwoPhaseFileHeader (xl_xact_prepare) — owned representation + byte codec
// ---------------------------------------------------------------------------

/// `xl_xact_prepare`, aka `TwoPhaseFileHeader` — the leading record of a 2PC
/// state file. Field order/types mirror the C struct so the byte codec
/// reproduces the on-disk / on-WAL layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TwoPhaseFileHeader {
    pub magic: u32,
    pub total_len: u32,
    pub xid: TransactionId,
    pub database: Oid,
    pub prepared_at: TimestampTz,
    pub owner: Oid,
    pub nsubxacts: i32,
    pub ncommitrels: i32,
    pub nabortrels: i32,
    pub ncommitstats: i32,
    pub nabortstats: i32,
    pub ninvalmsgs: i32,
    pub initfileinval: bool,
    pub gidlen: u16,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

impl TwoPhaseFileHeader {
    /// Wire size of `xl_xact_prepare`: header fields under natural alignment,
    /// with `origin_lsn` 8-aligned at offset 56 and `origin_timestamp` at 64.
    pub const fn wire_len() -> usize {
        72
    }

    pub fn to_bytes(&self) -> [u8; 72] {
        let mut b = [0u8; 72];
        b[0..4].copy_from_slice(&self.magic.to_le_bytes());
        b[4..8].copy_from_slice(&self.total_len.to_le_bytes());
        b[8..12].copy_from_slice(&self.xid.to_le_bytes());
        b[12..16].copy_from_slice(&self.database.to_le_bytes());
        b[16..24].copy_from_slice(&self.prepared_at.to_le_bytes());
        b[24..28].copy_from_slice(&self.owner.to_le_bytes());
        b[28..32].copy_from_slice(&self.nsubxacts.to_le_bytes());
        b[32..36].copy_from_slice(&self.ncommitrels.to_le_bytes());
        b[36..40].copy_from_slice(&self.nabortrels.to_le_bytes());
        b[40..44].copy_from_slice(&self.ncommitstats.to_le_bytes());
        b[44..48].copy_from_slice(&self.nabortstats.to_le_bytes());
        b[48..52].copy_from_slice(&self.ninvalmsgs.to_le_bytes());
        b[52] = self.initfileinval as u8;
        b[54..56].copy_from_slice(&self.gidlen.to_le_bytes());
        b[56..64].copy_from_slice(&self.origin_lsn.to_le_bytes());
        b[64..72].copy_from_slice(&self.origin_timestamp.to_le_bytes());
        b
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 72 {
            return None;
        }
        let rd_u32 = |o: usize| u32::from_le_bytes(buf[o..o + 4].try_into().unwrap());
        let rd_i32 = |o: usize| i32::from_le_bytes(buf[o..o + 4].try_into().unwrap());
        let rd_i64 = |o: usize| i64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
        let rd_u64 = |o: usize| u64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
        let rd_u16 = |o: usize| u16::from_le_bytes(buf[o..o + 2].try_into().unwrap());
        Some(TwoPhaseFileHeader {
            magic: rd_u32(0),
            total_len: rd_u32(4),
            xid: rd_u32(8),
            database: rd_u32(12),
            prepared_at: rd_i64(16),
            owner: rd_u32(24),
            nsubxacts: rd_i32(28),
            ncommitrels: rd_i32(32),
            nabortrels: rd_i32(36),
            ncommitstats: rd_i32(40),
            nabortstats: rd_i32(44),
            ninvalmsgs: rd_i32(48),
            initfileinval: buf[52] != 0,
            gidlen: rd_u16(54),
            origin_lsn: rd_u64(56),
            origin_timestamp: rd_i64(64),
        })
    }
}

/// `TwoPhaseRecordOnDisk` — `{ uint32 len; uint8 rmid; uint16 info; }`
/// (natural alignment → 8 bytes).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TwoPhaseRecordOnDisk {
    pub len: u32,
    pub rmid: u8,
    pub info: u16,
}

pub const SIZEOF_TWOPHASE_RECORD_ON_DISK: usize = 8;

impl TwoPhaseRecordOnDisk {
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut b = [0u8; 8];
        b[0..4].copy_from_slice(&self.len.to_le_bytes());
        b[4] = self.rmid;
        b[6..8].copy_from_slice(&self.info.to_le_bytes());
        b
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 8 {
            return None;
        }
        Some(TwoPhaseRecordOnDisk {
            len: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            rmid: buf[4],
            info: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
        })
    }
}

/// On-disk `RelFileLocator` width (`{ Oid; Oid; RelFileNumber; }` → 12 bytes).
pub const SIZEOF_REL_FILE_LOCATOR: usize = 12;
/// On-disk `xl_xact_stats_item` width (`{ int; Oid; uint64; }` → 16 bytes).
pub const SIZEOF_XL_XACT_STATS_ITEM: usize = 16;
/// On-disk `SharedInvalidationMessage` width (catcache-dominated union → 16).
pub const SIZEOF_SHARED_INVAL_MSG: usize = 16;

fn rel_file_locator_from_bytes(buf: &[u8]) -> RelFileLocator {
    let spc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let db = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let rel = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    RelFileLocator::new(spc, db, rel)
}

// ---------------------------------------------------------------------------
// SaveState — in-memory record assembly (`struct records` / save_state_data)
// ---------------------------------------------------------------------------

/// The in-memory state-file builder. In C this is the file-scope `records`
/// chunk chain; here it is an owned buffer charged to the current context.
pub struct SaveState {
    buf: Vec<u8>,
    pub num_chunks: u32,
    pub total_len: u32,
}

impl SaveState {
    pub fn new() -> Self {
        SaveState {
            buf: Vec::new(),
            num_chunks: 1,
            total_len: 0,
        }
    }

    /// `save_state_data(data, len)` — append `data`, padding to MAXALIGN.
    pub fn save_state_data(&mut self, data: &[u8]) -> PgResult<()> {
        let len = data.len();
        let padlen = maxalign(len);
        self.buf
            .try_reserve(padlen)
            .map_err(|_| oom_msg("appending two-phase state data"))?;
        self.buf.extend_from_slice(data);
        for _ in len..padlen {
            self.buf.push(0);
        }
        self.total_len += padlen as u32;
        Ok(())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn header_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

impl Default for SaveState {
    fn default() -> Self {
        Self::new()
    }
}

/// Out-of-memory `PgError` for a failed `try_reserve` on data-derived growth.
fn oom_msg(what: &str) -> PgError {
    ereport(ERROR)
        .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
        .errmsg(alloc::format!("out of memory {}", what))
        .into_error()
}

/// `RegisterTwoPhaseRecord` — append a 2PC record (header + optional payload).
pub fn register_two_phase_record(
    st: &mut SaveState,
    rmid: u8,
    info: u16,
    data: &[u8],
) -> PgResult<()> {
    let record = TwoPhaseRecordOnDisk {
        rmid,
        info,
        len: data.len() as u32,
    };
    st.save_state_data(&record.to_bytes())?;
    if !data.is_empty() {
        st.save_state_data(data)?;
    }
    Ok(())
}

/// Inputs to [`start_prepare`] — the transaction-private data the C function
/// gathers from `xactGetCommittedChildren` / `smgrGetPendingDeletes` /
/// `pgstat_get_transactional_drops` / `xactGetCommittedInvalidationMessages`.
/// Gathering those is the caller's (xact.c's) job; this crate owns assembly.
pub struct StartPrepareInput<'a> {
    pub xid: TransactionId,
    pub gid: &'a str,
    pub prepared_at: TimestampTz,
    pub owner: Oid,
    pub databaseid: Oid,
    pub children: &'a [TransactionId],
    /// Already-serialized `RelFileLocator[]` (12 bytes each) + element count.
    pub commitrels: &'a [u8],
    pub ncommitrels: i32,
    pub abortrels: &'a [u8],
    pub nabortrels: i32,
    /// Already-serialized `xl_xact_stats_item[]` bodies (16 bytes each).
    pub commitstats: &'a [u8],
    pub ncommitstats: i32,
    pub abortstats: &'a [u8],
    pub nabortstats: i32,
    /// Already-serialized `SharedInvalidationMessage[]` bodies.
    pub invalmsgs: &'a [u8],
    pub ninvalmsgs: i32,
    pub initfileinval: bool,
}

/// `StartPrepare` — initialize the builder and emit the file header + GID +
/// optional subxact/rel/stats/inval segments. The subxact data is also loaded
/// into the dummy PGPROC (`GXactLoadSubxactData`).
pub fn start_prepare(input: &StartPrepareInput, pgprocno: ProcNumber) -> PgResult<SaveState> {
    let mut st = SaveState::new();

    let gidlen = (input.gid.len() + 1) as u16; // include trailing NUL

    let hdr = TwoPhaseFileHeader {
        magic: TWOPHASE_MAGIC,
        total_len: 0, // end_prepare fills this
        xid: input.xid,
        database: input.databaseid,
        prepared_at: input.prepared_at,
        owner: input.owner,
        nsubxacts: input.children.len() as i32,
        ncommitrels: input.ncommitrels,
        nabortrels: input.nabortrels,
        ncommitstats: input.ncommitstats,
        nabortstats: input.nabortstats,
        ninvalmsgs: input.ninvalmsgs,
        initfileinval: input.initfileinval,
        gidlen,
        origin_lsn: InvalidXLogRecPtr,
        origin_timestamp: 0,
    };

    st.save_state_data(&hdr.to_bytes())?;

    let mut gidbuf = Vec::new();
    gidbuf
        .try_reserve(gidlen as usize)
        .map_err(|_| oom_msg("two-phase GID"))?;
    gidbuf.extend_from_slice(input.gid.as_bytes());
    gidbuf.push(0);
    st.save_state_data(&gidbuf)?;

    if !input.children.is_empty() {
        let mut sub = Vec::new();
        sub.try_reserve(input.children.len() * 4)
            .map_err(|_| oom_msg("two-phase subxacts"))?;
        for &c in input.children {
            sub.extend_from_slice(&c.to_le_bytes());
        }
        st.save_state_data(&sub)?;
        // While we have the child-xact data, stuff it in the gxact's PGPROC.
        proc::gxact_load_subxact_data::call(pgprocno, input.children)?;
    }
    if input.ncommitrels > 0 {
        st.save_state_data(input.commitrels)?;
    }
    if input.nabortrels > 0 {
        st.save_state_data(input.abortrels)?;
    }
    if input.ncommitstats > 0 {
        st.save_state_data(input.commitstats)?;
    }
    if input.nabortstats > 0 {
        st.save_state_data(input.abortstats)?;
    }
    if input.ninvalmsgs > 0 {
        st.save_state_data(input.invalmsgs)?;
    }

    Ok(st)
}


/// A snapshot of the replication-origin session globals
/// (`replorigin_session_origin`, `_lsn`, `_timestamp`). C reads these ambient
/// globals; passed explicitly so the owner is not pre-committed to ambient
/// state.
#[derive(Clone, Copy, Debug)]
pub struct ReplOriginSession {
    pub origin: RepOriginId,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

impl ReplOriginSession {
    fn active(&self) -> bool {
        self.origin != INVALID_REP_ORIGIN_ID && self.origin != DO_NOT_REPLICATE_ID
    }
}

/// `EndPrepare` — finish the builder, fill `total_len` and origin fields, check
/// the MaxAllocSize limit, drive the WAL insert, MarkAsPrepared, and SyncRep.
///
/// `slot` is the prepXacts index of the gxact being prepared. `repl` is the
/// replication-origin session snapshot the caller read off its own state.
pub fn end_prepare(
    state: &mut TwoPhaseStateData,
    slot: usize,
    mut builder: SaveState,
    repl: ReplOriginSession,
) -> PgResult<()> {
    register_two_phase_record(&mut builder, TWOPHASE_RM_END_ID, 0, &[])?;

    // sizeof(pg_crc32c) trailer.
    let total_len = builder.total_len + 4;
    {
        let hdr = builder.header_bytes_mut();
        debug_assert_eq!(
            u32::from_le_bytes(hdr[0..4].try_into().unwrap()),
            TWOPHASE_MAGIC
        );
        hdr[4..8].copy_from_slice(&total_len.to_le_bytes());
    }

    let replorigin = repl.active();
    if replorigin {
        let hdr = builder.header_bytes_mut();
        hdr[56..64].copy_from_slice(&repl.origin_lsn.to_le_bytes());
        hdr[64..72].copy_from_slice(&repl.origin_timestamp.to_le_bytes());
    }

    if total_len > MAX_ALLOC_SIZE {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("two-phase state file maximum length exceeded"));
    }

    wal::xlog_ensure_record_space::call(0, builder.num_chunks as i32)?;

    miscinit::start_crit_section::call();
    proc::set_delay_chkpt_start::call(true);

    let body = builder.into_vec();
    let prepare_end_lsn = wal::xlog_insert_prepare::call(&body)?;
    state[slot].prepare_end_lsn = prepare_end_lsn;

    if replorigin {
        origin::replorigin_session_advance::call(repl.origin_lsn, prepare_end_lsn)?;
    }

    wal::xlog_flush::call(prepare_end_lsn)?;

    state[slot].prepare_start_lsn = wal::proc_last_rec_ptr::call();

    mark_as_prepared(state, slot, false)?;

    proc::set_delay_chkpt_start::call(false);
    miscinit::end_crit_section::call();

    syncrep::sync_rep_wait_for_lsn::call(prepare_end_lsn, false)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// GlobalTransactionData / TwoPhaseStateData — flat #[repr(C)] cross-process
// shared-memory model
// ---------------------------------------------------------------------------
//
// In C `TwoPhaseStateData *TwoPhaseState` lives in the main shared-memory
// segment (`TwoPhaseShmemInit` → `ShmemInitStruct`) and every backend reaches
// the SAME bytes. A prepared transaction created by one backend must therefore
// be visible to any other backend (after a `\c -` reconnect the new backend
// must still see `pg_prepared_xacts` rows and be able to `COMMIT PREPARED`).
//
// This port places the genuinely-shared state at the real `ShmemInitStruct`
// address. Every `GlobalTransactionData` is `#[repr(C)]` (gid is a fixed
// `[u8; GIDSIZE]` array, NOT a `String`, so it lives in shmem) and the
// `prepXacts[]` active list + the freelist link through `gxact.next` mirror
// twophase.c exactly. `TwoPhaseStateData` is a thin handle holding the raw
// shared base pointer; its accessors index into the shared block.

/// `GlobalTransactionData` — one prepared (or preparing) global transaction.
/// `#[repr(C)]` so the array of these can live in genuine cross-process shared
/// memory. `next` is the freelist link (a prepXacts/gxacts index, or
/// `INVALID_GXACT_IDX` for the list tail), mirroring twophase.c's `next`
/// pointer. `pgprocno` indexes the dummy PGPROC.
#[repr(C)]
#[derive(Clone, Debug)]
pub struct GlobalTransactionData {
    pub pgprocno: ProcNumber,
    pub prepared_at: TimestampTz,
    pub prepare_start_lsn: XLogRecPtr,
    pub prepare_end_lsn: XLogRecPtr,
    pub xid: TransactionId,
    pub owner: Oid,
    pub locking_backend: ProcNumber,
    pub valid: bool,
    pub ondisk: bool,
    pub inredo: bool,
    /// Freelist link (gxact index) — `INVALID_GXACT_IDX` for the tail. Mirrors
    /// the C `GlobalTransactionData.next` pointer.
    next: i32,
    /// `gid[GIDSIZE]` plus the live length. A fixed array (not a `String`) so
    /// the struct is `#[repr(C)]` and shmem-placeable.
    gid_len: u16,
    gid_buf: [u8; GIDSIZE],
}

/// Freelist tail sentinel (the C `NULL` `next`).
const INVALID_GXACT_IDX: i32 = -1;

impl GlobalTransactionData {
    fn blank(pgprocno: ProcNumber) -> Self {
        GlobalTransactionData {
            pgprocno,
            prepared_at: 0,
            prepare_start_lsn: InvalidXLogRecPtr,
            prepare_end_lsn: InvalidXLogRecPtr,
            xid: InvalidTransactionId,
            owner: 0,
            locking_backend: INVALID_PROC_NUMBER,
            valid: false,
            ondisk: false,
            inredo: false,
            next: INVALID_GXACT_IDX,
            gid_len: 0,
            gid_buf: [0u8; GIDSIZE],
        }
    }

    /// The live GID as a `&str` (the bytes are always valid UTF-8 — GIDs are
    /// ASCII identifiers written via [`Self::set_gid`]).
    pub fn gid(&self) -> &str {
        // SAFETY: gid_buf[..gid_len] was written from a `&str` in set_gid.
        unsafe { core::str::from_utf8_unchecked(&self.gid_buf[..self.gid_len as usize]) }
    }

    /// Overwrite the GID (the C `strlcpy(gxact->gid, gid, GIDSIZE)`). Callers
    /// have already bounded `gid.len() < GIDSIZE`.
    pub fn set_gid(&mut self, gid: &str) {
        let bytes = gid.as_bytes();
        let n = bytes.len();
        self.gid_buf[..n].copy_from_slice(bytes);
        self.gid_len = n as u16;
    }
}

/// `#[repr(C)]` flat header for the shared `TwoPhaseStateData`. The flexible
/// `gxacts[]` array of [`GlobalTransactionData`] and the `prep_order[]` active
/// index list follow immediately after this header in the same shmem block, so
/// the whole structure is a single `ShmemInitStruct` allocation reachable by
/// every backend. `free_head` is the freelist head index (C `freeGXacts`).
#[repr(C)]
struct SharedHeader {
    free_head: i32,
    num_prep_xacts: u32,
    max_prepared_xacts: u32,
}

/// `TwoPhaseStateData` — a handle over the genuinely-shared 2PC state in main
/// shared memory. Holds only the raw base pointer (the `ShmemInitStruct`
/// address); all state lives in the shared block, so a fresh handle built in
/// any backend reaches the same prepared transactions.
pub struct TwoPhaseStateData {
    /// Base of the shared block (start of [`SharedHeader`]).
    base: *mut u8,
    pub max_prepared_xacts: usize,
}

// SAFETY: the handle is a single shared-memory pointer; the C model guards all
// access with TwoPhaseStateLock (an LWLock). Concurrency across backends is by
// real processes, not threads.
unsafe impl Send for TwoPhaseStateData {}

impl TwoPhaseStateData {
    /// Offset of the `prep_order[]` active-index array within the shared block
    /// (immediately after the maxaligned header).
    fn prep_order_off(&self) -> usize {
        maxalign(core::mem::size_of::<SharedHeader>())
    }

    /// Offset of the `gxacts[]` array within the shared block (after the
    /// maxaligned header + the maxaligned prep_order[] array).
    fn gxacts_off(&self) -> usize {
        let after_order =
            self.prep_order_off() + self.max_prepared_xacts * core::mem::size_of::<i32>();
        maxalign(after_order)
    }

    fn header(&self) -> &SharedHeader {
        // SAFETY: base points at a SharedHeader-sized-and-aligned shmem block.
        unsafe { &*(self.base as *const SharedHeader) }
    }

    fn header_mut(&mut self) -> &mut SharedHeader {
        // SAFETY: as `header`, exclusive via the caller's `&mut self`.
        unsafe { &mut *(self.base as *mut SharedHeader) }
    }

    fn gxact(&self, idx: usize) -> &GlobalTransactionData {
        debug_assert!(idx < self.max_prepared_xacts);
        // SAFETY: gxacts[] holds max_prepared_xacts GlobalTransactionData in the
        // shared block starting at gxacts_off().
        unsafe {
            &*(self.base.add(self.gxacts_off()) as *const GlobalTransactionData).add(idx)
        }
    }

    fn gxact_mut(&mut self, idx: usize) -> &mut GlobalTransactionData {
        debug_assert!(idx < self.max_prepared_xacts);
        let off = self.gxacts_off();
        // SAFETY: as `gxact`, exclusive via `&mut self`.
        unsafe { &mut *(self.base.add(off) as *mut GlobalTransactionData).add(idx) }
    }

    /// `prep_order[i]` — the gxacts index of the i-th active prepared xact.
    fn order(&self, i: usize) -> usize {
        debug_assert!(i < self.num_prep_xacts());
        // SAFETY: prep_order[] holds num_prep_xacts valid i32 indices.
        unsafe { *(self.base.add(self.prep_order_off()) as *const i32).add(i) as usize }
    }

    fn set_order(&mut self, i: usize, idx: usize) {
        debug_assert!(i < self.max_prepared_xacts);
        let off = self.prep_order_off();
        // SAFETY: i < max_prepared_xacts; the slot is within the prep_order[]
        // array bounds.
        unsafe { *(self.base.add(off) as *mut i32).add(i) = idx as i32 }
    }

    /// `TwoPhaseShmemInit` (`IsUnderPostmaster=false` branch): initialize the
    /// shared block — build the freelist of `max_prepared_xacts` gxact slots,
    /// each associated with its preallocated dummy PGPROC via
    /// `prepared_xact_procno`, and zero the active list.
    fn init_shared(&mut self) {
        let max = self.max_prepared_xacts;
        {
            let h = self.header_mut();
            h.num_prep_xacts = 0;
            h.free_head = INVALID_GXACT_IDX;
        }
        // C head-inserts the freelist (pop order i=max-1..0). Build i=0..max,
        // each linking to the previous head, so the head ends at max-1 and pops
        // descend exactly as C does.
        for i in 0..max {
            let procno = proc::prepared_xact_procno::call(i as i32);
            let prev_head = self.header().free_head;
            let g = self.gxact_mut(i);
            *g = GlobalTransactionData::blank(procno);
            g.next = prev_head;
            self.header_mut().free_head = i as i32;
        }
    }

    pub fn num_prep_xacts(&self) -> usize {
        self.header().num_prep_xacts as usize
    }

    pub fn prep_xact(&self, i: usize) -> &GlobalTransactionData {
        self.gxact(self.order(i))
    }

    pub fn prep_xact_mut(&mut self, i: usize) -> &mut GlobalTransactionData {
        let idx = self.order(i);
        self.gxact_mut(idx)
    }

    /// Pop the freelist head (the C `gxact = TwoPhaseState->freeGXacts;
    /// TwoPhaseState->freeGXacts = gxact->next;`). Returns the gxacts index.
    fn pop_free(&mut self) -> Option<usize> {
        let head = self.header().free_head;
        if head == INVALID_GXACT_IDX {
            return None;
        }
        let idx = head as usize;
        let next = self.gxact(idx).next;
        self.header_mut().free_head = next;
        Some(idx)
    }

    /// Append `idx` to the active `prepXacts[]` list, returning its slot.
    fn push_active(&mut self, idx: usize) -> usize {
        let slot = self.num_prep_xacts();
        self.set_order(slot, idx);
        self.header_mut().num_prep_xacts = (slot + 1) as u32;
        slot
    }
}

impl core::ops::Index<usize> for TwoPhaseStateData {
    type Output = GlobalTransactionData;
    fn index(&self, i: usize) -> &GlobalTransactionData {
        self.prep_xact(i)
    }
}
impl core::ops::IndexMut<usize> for TwoPhaseStateData {
    fn index_mut(&mut self, i: usize) -> &mut GlobalTransactionData {
        self.prep_xact_mut(i)
    }
}

/// `TwoPhaseShmemSize` — the shmem allocation size for `max_prepared_xacts`.
/// Mirrors the flat shared layout: a [`SharedHeader`], then the `prep_order[]`
/// active-index array, then the `gxacts[]` array of [`GlobalTransactionData`].
pub fn two_phase_shmem_size(max_prepared_xacts: usize) -> usize {
    let mut size = maxalign(core::mem::size_of::<SharedHeader>());
    size += max_prepared_xacts * core::mem::size_of::<i32>();
    size = maxalign(size);
    size += max_prepared_xacts * core::mem::size_of::<GlobalTransactionData>();
    size
}

// ---------------------------------------------------------------------------
// Registration / reservation (MarkAsPreparing, MarkAsPrepared, LockGXact, ...)
// ---------------------------------------------------------------------------

/// `MarkAsPreparing` — reserve the GID for `xid`. Returns its prepXacts index.
pub fn mark_as_preparing(
    state: &mut TwoPhaseStateData,
    twophase_exit_registered: &mut bool,
    my_locked_gxact: &mut Option<usize>,
    xid: TransactionId,
    gid: &str,
    prepared_at: TimestampTz,
    owner: Oid,
    databaseid: Oid,
) -> PgResult<usize> {
    if gid.len() >= GIDSIZE {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "transaction identifier \"{}\" is too long",
                gid
            )));
    }

    if state.max_prepared_xacts == 0 {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("prepared transactions are disabled")
            .errhint("Set \"max_prepared_transactions\" to a nonzero value."));
    }

    if !*twophase_exit_registered {
        register_twophase_exit()?;
        *twophase_exit_registered = true;
    }

    lwlock::lock_twophase_state::call(true)?;

    let result = (|| -> PgResult<usize> {
        for i in 0..state.num_prep_xacts() {
            if state.prep_xact(i).gid() == gid {
                return raise(ereport(ERROR)
                    .errcode(types_error::ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(alloc::format!(
                        "transaction identifier \"{}\" is already in use",
                        gid
                    )));
            }
        }

        let idx = match state.pop_free() {
            Some(idx) => idx,
            None => {
                return raise(ereport(ERROR)
                    .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
                    .errmsg("maximum number of prepared transactions reached")
                    .errhint(alloc::format!(
                        "Increase \"max_prepared_transactions\" (currently {}).",
                        state.max_prepared_xacts
                    )));
            }
        };

        let slot = state.push_active(idx);
        mark_as_preparing_guts(
            state,
            slot,
            my_locked_gxact,
            xid,
            gid,
            prepared_at,
            owner,
            databaseid,
        )?;
        state.prep_xact_mut(slot).ondisk = false;
        Ok(slot)
    })();

    lwlock::unlock_twophase_state::call()?;
    result
}

/// `MarkAsPreparingGuts` — fill the gxact and its dummy PGPROC.
fn mark_as_preparing_guts(
    state: &mut TwoPhaseStateData,
    slot: usize,
    my_locked_gxact: &mut Option<usize>,
    xid: TransactionId,
    gid: &str,
    prepared_at: TimestampTz,
    owner: Oid,
    databaseid: Oid,
) -> PgResult<()> {
    let pgprocno = state.prep_xact(slot).pgprocno;
    proc::proc_init_prepared::call(pgprocno, xid, owner, databaseid)?;

    let my_proc_number = proc::my_proc_number::call();
    let g = state.prep_xact_mut(slot);
    g.prepared_at = prepared_at;
    g.xid = xid;
    g.owner = owner;
    g.locking_backend = my_proc_number;
    g.valid = false;
    g.inredo = false;
    g.set_gid(gid);

    *my_locked_gxact = Some(slot);
    Ok(())
}

/// `MarkAsPrepared` — mark the gxact valid and enter its dummy proc into the
/// ProcArray.
pub fn mark_as_prepared(
    state: &mut TwoPhaseStateData,
    slot: usize,
    lock_held: bool,
) -> PgResult<()> {
    if !lock_held {
        lwlock::lock_twophase_state::call(true)?;
    }
    debug_assert!(!state.prep_xact(slot).valid);
    state.prep_xact_mut(slot).valid = true;
    if !lock_held {
        lwlock::unlock_twophase_state::call()?;
    }
    let pgprocno = state.prep_xact(slot).pgprocno;
    procarray::proc_array_add::call(pgprocno)
}

/// `LockGXact` — locate the prepared transaction by GID and mark it busy.
/// `user` is `GetUserId()`; `my_database_id` is `MyDatabaseId`, both read by
/// the caller off its own state.
pub fn lock_gxact(
    state: &mut TwoPhaseStateData,
    twophase_exit_registered: &mut bool,
    my_locked_gxact: &mut Option<usize>,
    gid: &str,
    user: Oid,
    my_database_id: Oid,
) -> PgResult<usize> {
    if !*twophase_exit_registered {
        register_twophase_exit()?;
        *twophase_exit_registered = true;
    }

    lwlock::lock_twophase_state::call(true)?;

    let outcome = (|| -> PgResult<Option<usize>> {
        for i in 0..state.num_prep_xacts() {
            let (valid, gxact_gid, locking_backend, owner, pgprocno) = {
                let g = state.prep_xact(i);
                (
                    g.valid,
                    g.gid().to_string(),
                    g.locking_backend,
                    g.owner,
                    g.pgprocno,
                )
            };
            if !valid {
                continue;
            }
            if gxact_gid != gid {
                continue;
            }

            if locking_backend != INVALID_PROC_NUMBER {
                return raise(ereport(ERROR)
                    .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(alloc::format!(
                        "prepared transaction with identifier \"{}\" is busy",
                        gid
                    )));
            }

            if user != owner && !miscinit::superuser_arg::call(user)? {
                return raise(ereport(ERROR)
                    .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg("permission denied to finish prepared transaction")
                    .errhint("Must be superuser or the user that prepared the transaction."));
            }

            if my_database_id != proc::proc_database_id::call(pgprocno) {
                return raise(ereport(ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("prepared transaction belongs to another database")
                    .errhint("Connect to the database where the transaction was prepared to finish it."));
            }

            let my_proc_number = proc::my_proc_number::call();
            state.prep_xact_mut(i).locking_backend = my_proc_number;
            *my_locked_gxact = Some(i);
            return Ok(Some(i));
        }
        Ok(None)
    })();

    lwlock::unlock_twophase_state::call()?;

    match outcome? {
        Some(i) => Ok(i),
        None => raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(alloc::format!(
                "prepared transaction with identifier \"{}\" does not exist",
                gid
            ))),
    }
}

/// `RemoveGXact` — remove the gxact at prepXacts `slot`, return its slot to the
/// freelist.
pub fn remove_gxact(state: &mut TwoPhaseStateData, slot: usize) -> PgResult<()> {
    if slot >= state.num_prep_xacts() {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_INTERNAL_ERROR)
            .errmsg("failed to find entry in GlobalTransaction array"));
    }
    // Swap-remove from prepXacts[]: move the last active entry into `slot` and
    // shrink the active count (the C `TwoPhaseState->prepXacts[i] =
    // TwoPhaseState->prepXacts[--TwoPhaseState->numPrepXacts];`).
    let backing = state.order(slot);
    let last = state.num_prep_xacts() - 1;
    if slot != last {
        let last_backing = state.order(last);
        state.set_order(slot, last_backing);
    }
    state.header_mut().num_prep_xacts = last as u32;
    // Return the gxact to the freelist head (C `gxact->next =
    // TwoPhaseState->freeGXacts; TwoPhaseState->freeGXacts = gxact;`).
    let prev_head = state.header().free_head;
    state.gxact_mut(backing).next = prev_head;
    state.header_mut().free_head = backing as i32;
    Ok(())
}

/// `GetPreparedTransactionList` — copy all gxacts under the shared lock,
/// returning the snapshot (the caller filters out not-yet-valid entries).
pub fn get_prepared_transaction_list(
    state: &TwoPhaseStateData,
) -> PgResult<Vec<GlobalTransactionData>> {
    lwlock::lock_twophase_state::call(false)?;
    let mut out = Vec::new();
    for i in 0..state.num_prep_xacts() {
        out.push(state.prep_xact(i).clone());
    }
    lwlock::unlock_twophase_state::call()?;
    Ok(out)
}

/// One output row of the `pg_prepared_xacts` view (the
/// `pg_prepared_xact` SRF's per-row projection): `(transaction, gid, prepared,
/// ownerid, dbid)`. The SRF's `FuncCallContext`/`SRF_RETURN_NEXT` plumbing and
/// the `heap_form_tuple` belong to the funcapi boundary; this is the owned
/// projection over the snapshot, with the not-yet-valid entries filtered out as
/// the C loop does (`if (!gxact->valid) continue;`).
#[derive(Clone, Debug)]
pub struct PreparedXactRow {
    pub transaction: TransactionId,
    pub gid: String,
    pub prepared: TimestampTz,
    pub ownerid: Oid,
    pub dbid: Oid,
}

/// `pg_prepared_xact` — the valid prepared-xact rows, projected. `proc->xid` and
/// `proc->databaseId` are read from the dummy PGPROC via proc seams (C uses
/// `GetPGProcByNumber(gxact->pgprocno)`).
pub fn pg_prepared_xact_rows(state: &TwoPhaseStateData) -> PgResult<Vec<PreparedXactRow>> {
    let list = get_prepared_transaction_list(state)?;
    let mut rows = Vec::new();
    for g in &list {
        if !g.valid {
            continue;
        }
        rows.push(PreparedXactRow {
            transaction: proc::proc_xid::call(g.pgprocno),
            gid: g.gid().to_string(),
            prepared: g.prepared_at,
            ownerid: g.owner,
            dbid: proc::proc_database_id::call(g.pgprocno),
        });
    }
    Ok(rows)
}

/// `AtAbort_Twophase` / `AtProcExit_Twophase` — release the gxact entry the
/// backend is working on (same logic for both; the exit hook calls abort).
pub fn at_abort_twophase(
    state: &mut TwoPhaseStateData,
    my_locked_gxact: &mut Option<usize>,
) -> PgResult<()> {
    let slot = match *my_locked_gxact {
        None => return Ok(()),
        Some(s) => s,
    };
    lwlock::lock_twophase_state::call(true)?;
    if !state.prep_xact(slot).valid {
        remove_gxact(state, slot)?;
    } else {
        state.prep_xact_mut(slot).locking_backend = INVALID_PROC_NUMBER;
    }
    lwlock::unlock_twophase_state::call()?;
    *my_locked_gxact = None;
    Ok(())
}

/// `PostPrepare_Twophase` — clear the locking backend after transfer is done.
pub fn post_prepare_twophase(
    state: &mut TwoPhaseStateData,
    my_locked_gxact: &mut Option<usize>,
) -> PgResult<()> {
    if let Some(slot) = *my_locked_gxact {
        lwlock::lock_twophase_state::call(true)?;
        state.prep_xact_mut(slot).locking_backend = INVALID_PROC_NUMBER;
        lwlock::unlock_twophase_state::call()?;
    }
    *my_locked_gxact = None;
    Ok(())
}

/// `TwoPhaseGetGXact` — find a gxact by XID, returning its prepXacts index.
pub fn two_phase_get_gxact(
    state: &TwoPhaseStateData,
    xid: TransactionId,
    lock_held: bool,
) -> PgResult<usize> {
    if !lock_held {
        lwlock::lock_twophase_state::call(false)?;
    }
    let mut result = None;
    for i in 0..state.num_prep_xacts() {
        if state.prep_xact(i).xid == xid {
            result = Some(i);
            break;
        }
    }
    if !lock_held {
        lwlock::unlock_twophase_state::call()?;
    }
    match result {
        Some(i) => Ok(i),
        None => raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!(
                "failed to find GlobalTransaction for xid {}",
                xid
            ))),
    }
}

/// `TwoPhaseGetDummyProcNumber`.
pub fn two_phase_get_dummy_proc_number(
    state: &TwoPhaseStateData,
    xid: TransactionId,
    lock_held: bool,
) -> PgResult<ProcNumber> {
    let slot = two_phase_get_gxact(state, xid, lock_held)?;
    Ok(state.prep_xact(slot).pgprocno)
}

/// `TwoPhaseGetXidByVirtualXID` — find a prepared xact by its dummy proc's VXID
/// `(procNumber, localTransactionId)`. Sets `*have_more` when >1 match.
pub fn two_phase_get_xid_by_virtual_xid(
    state: &TwoPhaseStateData,
    vxid: (ProcNumber, u32),
    have_more: &mut bool,
) -> PgResult<TransactionId> {
    *have_more = false;
    let mut result = InvalidTransactionId;
    lwlock::lock_twophase_state::call(false)?;
    for i in 0..state.num_prep_xacts() {
        let g = state.prep_xact(i);
        if !g.valid {
            continue;
        }
        let proc_vxid = proc::proc_vxid::call(g.pgprocno);
        if proc_vxid == vxid {
            debug_assert!(!g.inredo);
            if result != InvalidTransactionId {
                *have_more = true;
                break;
            }
            result = g.xid;
        }
    }
    lwlock::unlock_twophase_state::call()?;
    Ok(result)
}

// ---------------------------------------------------------------------------
// State-file read / recreate (CRC + magic + length validation, in-crate)
// ---------------------------------------------------------------------------

/// `ReadTwoPhaseFile` — read and validate the 2PC state file for `xid`. The raw
/// file bytes are fetched via the file-I/O seam; magic/total_len/CRC/alignment
/// validation is done here. Returns `None` when `missing_ok` and absent.
pub fn read_twophase_file(xid: TransactionId, missing_ok: bool) -> PgResult<Option<Vec<u8>>> {
    let buf = match files::read_twophase_file::call(xid, missing_ok)? {
        Some(b) => b,
        None => return Ok(None),
    };

    let st_size = buf.len();
    let lower_bound =
        maxalign(TwoPhaseFileHeader::wire_len()) + maxalign(SIZEOF_TWOPHASE_RECORD_ON_DISK) + 4;
    if st_size < lower_bound || (st_size as u32) > MAX_ALLOC_SIZE {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg(alloc::format!(
                "incorrect size of two-phase state file: {} bytes",
                st_size
            )));
    }

    let crc_offset = st_size - 4;
    if crc_offset != maxalign(crc_offset) {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("incorrect alignment of CRC offset for two-phase state file"));
    }

    let hdr = TwoPhaseFileHeader::from_bytes(&buf).ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("two-phase state file too short for header")
            .into_error()
    })?;
    if hdr.magic != TWOPHASE_MAGIC {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("invalid magic number stored in two-phase state file"));
    }
    if hdr.total_len as usize != st_size {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("invalid size stored in two-phase state file"));
    }

    let mut calc = init_crc32c();
    calc = comp_crc32c(calc, &buf[..crc_offset]);
    calc = fin_crc32c(calc);
    let file_crc = u32::from_le_bytes(buf[crc_offset..crc_offset + 4].try_into().unwrap());
    if !eq_crc32c(calc, file_crc) {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg(
                "calculated CRC checksum does not match value stored in two-phase state file",
            ));
    }

    Ok(Some(buf))
}

/// `XlogReadTwoPhaseData(lsn, &buf, &len)` (twophase.c:1404). Re-read the
/// prepare-record body from WAL at `lsn`. Used when COMMIT/ABORT PREPARED runs
/// before the prepare record's checkpoint has moved the state to disk, and by
/// `CheckPointTwoPhase`/recovery. Allocates a throwaway [`XLogReader`] over the
/// local pg_wal files (the stock `read_local_xlog_page` routine), reads exactly
/// one record at `lsn`, validates it is an `RM_XACT_ID`/`XLOG_XACT_PREPARE`
/// record, and returns its rmgr data bytes.
pub fn xlog_read_twophase_data(lsn: XLogRecPtr) -> PgResult<Vec<u8>> {
    use types_logical::XLogReaderRoutineHandle;

    // XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(...), NULL).
    let wal_segment_size = wal::wal_segment_size::call();
    let reader = xlogreader::XLogReaderAllocate::call(
        wal_segment_size,
        XLogReaderRoutineHandle::default(),
    )
    .ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
            .errmsg("out of memory")
            .errdetail("Failed while allocating a WAL reading processor.")
            .into_error()
    })?;

    // Guard so the reader's context is always freed, even on the error paths.
    let result = (|| -> PgResult<Vec<u8>> {
        xlogreader::XLogBeginRead::call(reader, lsn);
        let read = xlogreader::XLogReadRecord::call(reader);

        let (h, l) = ((lsn >> 32) as u32, lsn as u32);
        if !read.record {
            return match read.err {
                Some(errormsg) => raise(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(alloc::format!(
                        "could not read two-phase state from WAL at {h:X}/{l:X}: {errormsg}"
                    ))),
                None => raise(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(alloc::format!(
                        "could not read two-phase state from WAL at {h:X}/{l:X}"
                    ))),
            };
        }

        let rmid = xlogreader::xlog_rec_get_rmid::call(reader);
        let info = xlogreader::xlog_rec_get_info::call(reader);
        if rmid != ::wal::wal::RM_XACT_ID
            || (info & ::wal::xact::XLOG_XACT_OPMASK) != ::wal::xact::XLOG_XACT_PREPARE
        {
            return raise(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(alloc::format!(
                    "expected two-phase state data is not present in WAL at {h:X}/{l:X}"
                )));
        }

        Ok(xlogreader::xlog_rec_get_main_data::call(reader))
    })();

    // XLogReaderFree(xlogreader): always reclaim the reader + its context.
    xlogreader::XLogReaderFree::call(reader);
    result
}

/// `RecreateTwoPhaseFile` — compute the CRC over `content` and write the file.
pub fn recreate_two_phase_file(xid: TransactionId, content: &[u8]) -> PgResult<()> {
    let mut crc = init_crc32c();
    crc = comp_crc32c(crc, content);
    crc = fin_crc32c(crc);
    files::recreate_twophase_file::call(xid, content, crc)
}

/// `StandbyTransactionIdIsPrepared` — confirm `xid` is prepared (recovery).
pub fn standby_transaction_id_is_prepared(
    xid: TransactionId,
    max_prepared_xacts: usize,
) -> PgResult<bool> {
    debug_assert!(TransactionIdIsValid(xid));
    if max_prepared_xacts == 0 {
        return Ok(false);
    }
    let buf = match read_twophase_file(xid, true)? {
        Some(b) => b,
        None => return Ok(false),
    };
    let hdr = TwoPhaseFileHeader::from_bytes(&buf).ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupted two-phase state file header")
            .into_error()
    })?;
    Ok(hdr.xid == xid)
}

// ---------------------------------------------------------------------------
// ProcessRecords — rmgr callback dispatch over an in-memory 2PC buffer
// ---------------------------------------------------------------------------

/// The rmgr-callback table for a `ProcessRecords` walk. `phase` selects the
/// recover / post-commit / post-abort table (the `twophase_rmgr` unit owns the
/// table contents).
fn callbacks_for(phase: u8) -> &'static [Option<TwoPhaseCallback>; rmgrcb::NUM_TWOPHASE_RM] {
    match phase {
        TWOPHASE_PHASE_RECOVER => &rmgrcb::twophase_recover_callbacks,
        TWOPHASE_PHASE_POSTCOMMIT => &rmgrcb::twophase_postcommit_callbacks,
        TWOPHASE_PHASE_POSTABORT => &rmgrcb::twophase_postabort_callbacks,
        _ => unreachable!("invalid two-phase ProcessRecords phase"),
    }
}

/// `ProcessRecords` — walk the 2PC records starting at `off` into `buf` and
/// invoke `callbacks[rmid]` for each (when non-`NULL`), until the END sentinel.
pub fn process_records(buf: &[u8], mut off: usize, xid: TransactionId, phase: u8) -> PgResult<()> {
    let callbacks = callbacks_for(phase);
    loop {
        let record = TwoPhaseRecordOnDisk::from_bytes(&buf[off..]).ok_or_else(|| {
            ereport(ERROR)
                .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                .errmsg("truncated two-phase record")
                .into_error()
        })?;
        debug_assert!(record.rmid <= TWOPHASE_RM_MAX_ID);
        if record.rmid == TWOPHASE_RM_END_ID {
            break;
        }
        off += maxalign(SIZEOF_TWOPHASE_RECORD_ON_DISK);
        let datalen = record.len as usize;
        let recdata = &buf[off..off + datalen];
        if let Some(cb) = callbacks[record.rmid as usize] {
            cb(xid, record.info, recdata)?;
        }
        off += maxalign(datalen);
    }
    Ok(())
}

/// The byte offsets of each 2PC-file segment, computed from the header counts.
#[derive(Clone, Copy, Debug)]
pub struct BufferLayout {
    pub gid: usize,
    pub children: usize,
    pub commitrels: usize,
    pub abortrels: usize,
    pub commitstats: usize,
    pub abortstats: usize,
    pub invalmsgs: usize,
    pub records: usize,
}

impl BufferLayout {
    pub fn of(hdr: &TwoPhaseFileHeader) -> BufferLayout {
        let mut off = maxalign(TwoPhaseFileHeader::wire_len());
        let gid = off;
        off += maxalign(hdr.gidlen as usize);
        let children = off;
        off += maxalign(hdr.nsubxacts as usize * 4);
        let commitrels = off;
        off += maxalign(hdr.ncommitrels as usize * SIZEOF_REL_FILE_LOCATOR);
        let abortrels = off;
        off += maxalign(hdr.nabortrels as usize * SIZEOF_REL_FILE_LOCATOR);
        let commitstats = off;
        off += maxalign(hdr.ncommitstats as usize * SIZEOF_XL_XACT_STATS_ITEM);
        let abortstats = off;
        off += maxalign(hdr.nabortstats as usize * SIZEOF_XL_XACT_STATS_ITEM);
        let invalmsgs = off;
        off += maxalign(hdr.ninvalmsgs as usize * SIZEOF_SHARED_INVAL_MSG);
        BufferLayout {
            gid,
            children,
            commitrels,
            abortrels,
            commitstats,
            abortstats,
            invalmsgs,
            records: off,
        }
    }
}

/// Decode the subxact XID array from the children segment.
pub fn decode_children(buf: &[u8], layout: &BufferLayout, n: usize) -> Vec<TransactionId> {
    let mut v = Vec::with_capacity(n);
    let base = layout.children;
    for i in 0..n {
        let o = base + i * 4;
        v.push(u32::from_le_bytes(buf[o..o + 4].try_into().unwrap()));
    }
    v
}

/// Decode a RelFileLocator array from a segment beginning at `base`.
pub fn decode_rels(buf: &[u8], base: usize, n: usize) -> Vec<RelFileLocator> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = base + i * SIZEOF_REL_FILE_LOCATOR;
        v.push(rel_file_locator_from_bytes(
            &buf[o..o + SIZEOF_REL_FILE_LOCATOR],
        ));
    }
    v
}

/// Decode an `xl_xact_stats_item[]` segment (16 bytes each:
/// `{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`) from `bytes`.
fn decode_stats_items(bytes: &[u8], n: usize) -> Vec<types_core::xact::XlXactStatsItem> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = i * SIZEOF_XL_XACT_STATS_ITEM;
        let kind = i32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
        let dboid = u32::from_le_bytes(bytes[o + 4..o + 8].try_into().unwrap());
        let objid_lo = u32::from_le_bytes(bytes[o + 8..o + 12].try_into().unwrap());
        let objid_hi = u32::from_le_bytes(bytes[o + 12..o + 16].try_into().unwrap());
        let objid = ((objid_hi as u64) << 32) | objid_lo as u64;
        v.push(types_core::xact::XlXactStatsItem { kind, dboid, objid });
    }
    v
}

fn decode_gid(buf: &[u8], layout: &BufferLayout, hdr: &TwoPhaseFileHeader) -> PgResult<String> {
    let g = &buf[layout.gid..layout.gid + hdr.gidlen as usize];
    let end = g.iter().position(|&b| b == 0).unwrap_or(g.len());
    core::str::from_utf8(&g[..end])
        .map(String::from)
        .map_err(|_| {
            ereport(ERROR)
                .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                .errmsg("invalid UTF-8 in two-phase GID")
                .into_error()
        })
}

// ---------------------------------------------------------------------------
// FinishPreparedTransaction (COMMIT PREPARED / ROLLBACK PREPARED)
// ---------------------------------------------------------------------------

/// Backend identity + replication-origin snapshot the caller reads off its own
/// state and hands to [`finish_prepared_transaction`].
#[derive(Clone, Copy, Debug)]
pub struct FinishContext {
    pub user_id: Oid,
    pub my_database_id: Oid,
    pub repl: ReplOriginSession,
    pub current_timestamp: TimestampTz,
    /// `TransactionXmin` (snapmgr.c) — needed by `TransactionIdDidCommit`'s
    /// subtrans recursion; read by the caller off its snapshot state.
    pub transaction_xmin: TransactionId,
    /// `MyXactFlags` (xact.c) — the committing backend's accumulated xact
    /// flags, OR'd with `XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK` into the 2nd
    /// phase commit/abort record (an ambient global threaded as a param).
    pub my_xact_flags: i32,
    /// `MyDatabaseTableSpace` (globals.c) — written into the commit record's
    /// `xl_xact_dbinfo` when the record carries inval messages or logical info.
    pub my_database_table_space: Oid,
    /// `XLogLogicalInfoActive()` — whether logical decoding info is being
    /// emitted (affects `xinfo`/`HAS_GID`); read by the caller.
    pub xlog_logical_info_active: bool,
}

/// `FinishPreparedTransaction` — execute COMMIT PREPARED or ROLLBACK PREPARED.
pub fn finish_prepared_transaction(
    state: &mut TwoPhaseStateData,
    twophase_exit_registered: &mut bool,
    my_locked_gxact: &mut Option<usize>,
    gid: &str,
    is_commit: bool,
    ctx: FinishContext,
) -> PgResult<()> {
    let slot = lock_gxact(
        state,
        twophase_exit_registered,
        my_locked_gxact,
        gid,
        ctx.user_id,
        ctx.my_database_id,
    )?;
    let (pgprocno, xid, ondisk_at_lock, prepare_start_lsn) = {
        let g = state.prep_xact(slot);
        (g.pgprocno, g.xid, g.ondisk, g.prepare_start_lsn)
    };

    let buf = if ondisk_at_lock {
        read_twophase_file(xid, false)?.ok_or_else(|| {
            ereport(ERROR)
                .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                .errmsg("two-phase state file disappeared")
                .into_error()
        })?
    } else {
        wal::xlog_read_twophase_data::call(prepare_start_lsn)?
    };

    let hdr = TwoPhaseFileHeader::from_bytes(&buf).ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupted two-phase state buffer")
            .into_error()
    })?;
    debug_assert_eq!(hdr.xid, xid);

    let layout = BufferLayout::of(&hdr);
    let children = decode_children(&buf, &layout, hdr.nsubxacts as usize);
    let commitrels = decode_rels(&buf, layout.commitrels, hdr.ncommitrels as usize);
    let abortrels = decode_rels(&buf, layout.abortrels, hdr.nabortrels as usize);
    let commitstats =
        &buf[layout.commitstats..layout.commitstats + hdr.ncommitstats as usize * SIZEOF_XL_XACT_STATS_ITEM];
    let abortstats =
        &buf[layout.abortstats..layout.abortstats + hdr.nabortstats as usize * SIZEOF_XL_XACT_STATS_ITEM];
    let invalmsgs =
        &buf[layout.invalmsgs..layout.invalmsgs + hdr.ninvalmsgs as usize * SIZEOF_SHARED_INVAL_MSG];

    let latest_xid = transaction_id_latest(xid, &children);

    miscinit::hold_interrupts::call();

    // Order is critical: WAL record, then clog, then ProcArray removal, then
    // callbacks.
    if is_commit {
        record_transaction_commit_prepared(
            xid,
            &children,
            &commitrels,
            decode_stats_items(commitstats, hdr.ncommitstats as usize),
            invalmsgs,
            hdr.ninvalmsgs,
            hdr.initfileinval,
            gid,
            ctx,
        )?;
    } else {
        record_transaction_abort_prepared(
            xid,
            &children,
            &abortrels,
            decode_stats_items(abortstats, hdr.nabortstats as usize),
            gid,
            ctx,
        )?;
    }

    procarray::proc_array_remove::call(pgprocno, latest_xid)?;

    state.prep_xact_mut(slot).valid = false;

    let delrels = if is_commit { &commitrels } else { &abortrels };
    storage_smgr::drop_relation_files::call(delrels)?;

    if is_commit {
        pgstat::pgstat_execute_transactional_drops::call(commitstats, hdr.ncommitstats)?;
    } else {
        pgstat::pgstat_execute_transactional_drops::call(abortstats, hdr.nabortstats)?;
    }

    if is_commit {
        if hdr.initfileinval {
            inval::relcache_init_file_pre_invalidate::call()?;
        }
        inval::send_shared_invalid_messages::call(invalmsgs, hdr.ninvalmsgs)?;
        if hdr.initfileinval {
            inval::relcache_init_file_post_invalidate::call()?;
        }
    }

    lwlock::lock_twophase_state::call(true)?;

    let phase = if is_commit {
        TWOPHASE_PHASE_POSTCOMMIT
    } else {
        TWOPHASE_PHASE_POSTABORT
    };
    let cb_result = process_records(&buf, layout.records, xid, phase)
        .and_then(|()| predicate::predicate_lock_twophase_finish::call(xid, is_commit));

    let ondisk = state.prep_xact(slot).ondisk;

    let remove_result = remove_gxact(state, slot);

    lwlock::unlock_twophase_state::call()?;
    cb_result?;
    remove_result?;

    pgstat::at_eoxact_pgstat::call(is_commit)?;

    if ondisk {
        files::remove_twophase_file::call(xid, true)?;
    }

    *my_locked_gxact = None;
    miscinit::resume_interrupts::call();
    Ok(())
}

/// `TransactionIdLatest(xid, children)` — numerically-latest under modular
/// comparison (transam.c).
fn transaction_id_latest(xid: TransactionId, children: &[TransactionId]) -> TransactionId {
    let mut result = xid;
    for &c in children {
        if transaction_id_follows(c, result) {
            result = c;
        }
    }
    result
}

fn transaction_id_follows(a: TransactionId, b: TransactionId) -> bool {
    if !TransactionIdIsValid(a) || !TransactionIdIsValid(b) {
        return a > b;
    }
    let diff = a.wrapping_sub(b) as i32;
    diff > 0
}

fn transaction_id_precedes(a: TransactionId, b: TransactionId) -> bool {
    if !TransactionIdIsValid(a) || !TransactionIdIsValid(b) {
        return a < b;
    }
    let diff = a.wrapping_sub(b) as i32;
    diff < 0
}

fn transaction_id_follows_or_equals(a: TransactionId, b: TransactionId) -> bool {
    if !TransactionIdIsValid(a) || !TransactionIdIsValid(b) {
        return a >= b;
    }
    let diff = a.wrapping_sub(b) as i32;
    diff >= 0
}

// ---------------------------------------------------------------------------
// RecordTransactionCommitPrepared / RecordTransactionAbortPrepared
// ---------------------------------------------------------------------------

fn record_transaction_commit_prepared(
    xid: TransactionId,
    children: &[TransactionId],
    rels: &[RelFileLocator],
    dropped_stats: Vec<types_core::xact::XlXactStatsItem>,
    invalmsgs_bytes: &[u8],
    ninvalmsgs: i32,
    initfileinval: bool,
    gid: &str,
    ctx: FinishContext,
) -> PgResult<()> {
    let repl = ctx.repl;
    let committs = ctx.current_timestamp;
    let replorigin = repl.active();
    let mut origin_ts = repl.origin_timestamp;

    miscinit::start_crit_section::call();
    proc::set_delay_chkpt_start::call(true);

    let args = XactLogCommitRecordArgs {
        commit_time: committs,
        subxacts: children.to_vec(),
        rels: rels.to_vec(),
        dropped_stats,
        msgs: invalmsgs_bytes.to_vec(),
        nmsgs: ninvalmsgs,
        relcache_inval: initfileinval,
        xactflags: ctx.my_xact_flags | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
        twophase_xid: xid,
        twophase_gid: Some(String::from(gid)),
        force_sync_commit: false,
        synchronous_commit: 0,
        xlog_logical_info_active: ctx.xlog_logical_info_active,
        my_database_id: ctx.my_database_id,
        my_database_table_space: ctx.my_database_table_space,
        replorigin_session_origin: repl.origin,
        origin: if replorigin {
            Some(XlXactOrigin {
                origin_lsn: repl.origin_lsn,
                origin_timestamp: origin_ts,
            })
        } else {
            None
        },
    };
    let recptr = xact::xact_log_commit_record::call(&args)?;

    if replorigin {
        origin::replorigin_session_advance::call(repl.origin_lsn, xact::xact_last_rec_end::call())?;
    }

    if !replorigin || origin_ts == 0 {
        origin_ts = committs;
        origin::set_replorigin_session_timestamp::call(origin_ts);
    }

    commit_ts::transaction_tree_set_commit_ts_data::call(xid, children, origin_ts, repl.origin)?;

    wal::xlog_flush::call(recptr)?;

    transam::TransactionIdCommitTree(xid, children)?;

    proc::set_delay_chkpt_start::call(false);
    miscinit::end_crit_section::call();

    syncrep::sync_rep_wait_for_lsn::call(recptr, true)?;
    Ok(())
}

fn record_transaction_abort_prepared(
    xid: TransactionId,
    children: &[TransactionId],
    rels: &[RelFileLocator],
    dropped_stats: Vec<types_core::xact::XlXactStatsItem>,
    gid: &str,
    ctx: FinishContext,
) -> PgResult<()> {
    let repl = ctx.repl;
    let abort_time = ctx.current_timestamp;
    let transaction_xmin = ctx.transaction_xmin;
    let replorigin = repl.active();

    // Catch the abort-after-commit scenario.
    if transam::TransactionIdDidCommit(xid, transaction_xmin)? {
        return raise(ereport(PANIC).errmsg(alloc::format!(
            "cannot abort transaction {}, it was already committed",
            xid
        )));
    }

    miscinit::start_crit_section::call();

    let args = XactLogAbortRecordArgs {
        abort_time,
        subxacts: children.to_vec(),
        rels: rels.to_vec(),
        dropped_stats,
        xactflags: ctx.my_xact_flags | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
        twophase_xid: xid,
        twophase_gid: Some(String::from(gid)),
        xlog_logical_info_active: ctx.xlog_logical_info_active,
        my_database_id: ctx.my_database_id,
        my_database_table_space: ctx.my_database_table_space,
        replorigin_session_origin: repl.origin,
        origin: if replorigin {
            Some(XlXactOrigin {
                origin_lsn: repl.origin_lsn,
                origin_timestamp: repl.origin_timestamp,
            })
        } else {
            None
        },
    };
    let recptr = xact::xact_log_abort_record::call(&args)?;

    if replorigin {
        origin::replorigin_session_advance::call(repl.origin_lsn, xact::xact_last_rec_end::call())?;
    }

    wal::xlog_flush::call(recptr)?;

    transam::TransactionIdAbortTree(xid, children)?;

    miscinit::end_crit_section::call();

    syncrep::sync_rep_wait_for_lsn::call(recptr, false)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Redo + recovery (PrepareRedoAdd/Remove, scans, ProcessTwoPhaseBuffer)
// ---------------------------------------------------------------------------

/// `PrepareRedoAdd` — register a gxact in redo from the prepare-record `buf`.
/// `reached_consistency` is the recovery state the caller passes in.
pub fn prepare_redo_add(
    state: &mut TwoPhaseStateData,
    buf: &[u8],
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    origin_id: RepOriginId,
    reached_consistency: bool,
) -> PgResult<()> {
    debug_assert!(lwlock::twophase_state_held_exclusive::call());

    let hdr = TwoPhaseFileHeader::from_bytes(buf).ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupted prepare record header")
            .into_error()
    })?;
    let layout = BufferLayout::of(&hdr);
    let gid = decode_gid(buf, &layout, &hdr)?;

    // If the 2PC data already found its way to disk, skip to avoid duplicates.
    if !xlog_rec_ptr_is_invalid(start_lsn) && files::twophase_file_exists::call(hdr.xid)? {
        let level = if reached_consistency { ERROR } else { WARNING };
        // ERROR propagates as Err; WARNING logs and we return Ok (the C code
        // `return`s after the ereport regardless of level).
        ereport(level)
            .errmsg(alloc::format!(
                "could not recover two-phase state file for transaction {}",
                hdr.xid
            ))
            .errdetail("Two-phase state file has been found in WAL record, but this transaction has already been restored from disk.")
            .finish(here())?;
        return Ok(());
    }

    let idx = match state.pop_free() {
        Some(idx) => idx,
        None => {
            return raise(ereport(ERROR)
                .errcode(types_error::ERRCODE_OUT_OF_MEMORY)
                .errmsg("maximum number of prepared transactions reached")
                .errhint(alloc::format!(
                    "Increase \"max_prepared_transactions\" (currently {}).",
                    state.max_prepared_xacts
                )));
        }
    };
    let slot = state.push_active(idx);
    {
        let g = state.prep_xact_mut(slot);
        g.prepared_at = hdr.prepared_at;
        g.prepare_start_lsn = start_lsn;
        g.prepare_end_lsn = end_lsn;
        g.xid = hdr.xid;
        g.owner = hdr.owner;
        g.locking_backend = INVALID_PROC_NUMBER;
        g.valid = false;
        g.ondisk = xlog_rec_ptr_is_invalid(start_lsn);
        g.inredo = true;
        g.set_gid(&gid);
    }

    if origin_id != INVALID_REP_ORIGIN_ID {
        origin::replorigin_advance::call(origin_id, hdr.origin_lsn, end_lsn, false, false)?;
    }
    Ok(())
}

/// `PrepareRedoRemove` — remove a redo-added gxact (and its file, if on disk).
pub fn prepare_redo_remove(
    state: &mut TwoPhaseStateData,
    xid: TransactionId,
    give_warning: bool,
) -> PgResult<()> {
    debug_assert!(lwlock::twophase_state_held_exclusive::call());

    let mut found = None;
    for i in 0..state.num_prep_xacts() {
        if state.prep_xact(i).xid == xid {
            debug_assert!(state.prep_xact(i).inredo);
            found = Some(i);
            break;
        }
    }
    let slot = match found {
        None => return Ok(()),
        Some(s) => s,
    };
    if state.prep_xact(slot).ondisk {
        files::remove_twophase_file::call(xid, give_warning)?;
    }
    remove_gxact(state, slot)
}

/// `ProcessTwoPhaseBuffer` — read a prepared-xact buffer (from disk or WAL),
/// validate, and (per flags) set subxact parents / advance nextXid. `orig_next_xid`
/// is `XidFromFullTransactionId(TransamVariables->nextXid)`, read by the caller.
pub fn process_two_phase_buffer(
    state: &mut TwoPhaseStateData,
    xid: TransactionId,
    prepare_start_lsn: XLogRecPtr,
    fromdisk: bool,
    set_parent: bool,
    set_next_xid: bool,
    orig_next_xid: TransactionId,
    transaction_xmin: TransactionId,
) -> PgResult<Option<Vec<u8>>> {
    debug_assert!(lwlock::twophase_state_held_exclusive::call());

    if !fromdisk {
        debug_assert!(prepare_start_lsn != InvalidXLogRecPtr);
    }

    // Already processed?
    if transam::TransactionIdDidCommit(xid, transaction_xmin)?
        || transam::TransactionIdDidAbort(xid, transaction_xmin)?
    {
        if fromdisk {
            files::remove_twophase_file::call(xid, true)?;
        } else {
            prepare_redo_remove(state, xid, true)?;
        }
        return Ok(None);
    }

    // Reject XID if too new.
    if transaction_id_follows_or_equals(xid, orig_next_xid) {
        if fromdisk {
            files::remove_twophase_file::call(xid, true)?;
        } else {
            prepare_redo_remove(state, xid, true)?;
        }
        return Ok(None);
    }

    let buf = if fromdisk {
        read_twophase_file(xid, false)?.ok_or_else(|| {
            ereport(ERROR)
                .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                .errmsg("two-phase state file disappeared")
                .into_error()
        })?
    } else {
        wal::xlog_read_twophase_data::call(prepare_start_lsn)?
    };

    let hdr = TwoPhaseFileHeader::from_bytes(&buf).ok_or_else(|| {
        ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg("corrupted two-phase state buffer")
            .into_error()
    })?;
    if hdr.xid != xid {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
            .errmsg(alloc::format!(
                "corrupted two-phase state for transaction {}",
                xid
            )));
    }

    let layout = BufferLayout::of(&hdr);
    let subxids = decode_children(&buf, &layout, hdr.nsubxacts as usize);
    for &subxid in &subxids {
        debug_assert!(transaction_id_follows(subxid, xid));
        if set_next_xid {
            varsup::advance_next_full_xid_past_xid::call(subxid)?;
        }
        if set_parent {
            subtrans::sub_trans_set_parent::call(subxid, xid)?;
        }
    }

    Ok(Some(buf))
}

/// `restoreTwoPhaseData` — scan `pg_twophase`, validating each file and adding
/// it via `prepare_redo_add`. `orig_next_xid` is read by the caller.
pub fn restore_two_phase_data(
    state: &mut TwoPhaseStateData,
    orig_next_xid: TransactionId,
    transaction_xmin: TransactionId,
    reached_consistency: bool,
) -> PgResult<()> {
    lwlock::lock_twophase_state::call(true)?;
    let result = (|| -> PgResult<()> {
        let names = files::scan_twophase_dir::call()?;
        for fxid in names {
            let xid = fxid as u32; // XidFromFullTransactionId
            let buf =
                process_two_phase_buffer(state, xid, InvalidXLogRecPtr, true, false, false, orig_next_xid, transaction_xmin)?;
            if let Some(buf) = buf {
                prepare_redo_add(
                    state,
                    &buf,
                    InvalidXLogRecPtr,
                    InvalidXLogRecPtr,
                    INVALID_REP_ORIGIN_ID,
                    reached_consistency,
                )?;
            }
        }
        Ok(())
    })();
    lwlock::unlock_twophase_state::call()?;
    result
}

/// `PrescanPreparedTransactions` — scan `state`, determine the valid XID range,
/// and return `(oldest_valid_xid, xids)`. `orig_next_xid` is read by the caller.
pub fn prescan_prepared_transactions(
    state: &mut TwoPhaseStateData,
    orig_next_xid: TransactionId,
    transaction_xmin: TransactionId,
) -> PgResult<(TransactionId, Vec<TransactionId>)> {
    let mut result = orig_next_xid;
    let mut xids = Vec::new();

    lwlock::lock_twophase_state::call(true)?;
    let inner = (|| -> PgResult<()> {
        // C iterates `for (i = 0; i < numPrepXacts; i++)` with a plain `i++`
        // after each entry. When ProcessTwoPhaseBuffer removes the current
        // entry (PrepareRedoRemove swap-removes index i and decrements
        // numPrepXacts), the entry swapped into slot i is left UNscanned and
        // the loop terminates one earlier — we must reproduce that exactly,
        // so always advance `i` regardless of whether a removal happened.
        let mut i = 0;
        while i < state.num_prep_xacts() {
            let (xid, start_lsn, ondisk) = {
                let g = state.prep_xact(i);
                debug_assert!(g.inredo);
                (g.xid, g.prepare_start_lsn, g.ondisk)
            };
            let buf =
                process_two_phase_buffer(state, xid, start_lsn, ondisk, false, true, orig_next_xid, transaction_xmin)?;
            if buf.is_some() {
                if transaction_id_precedes(xid, result) {
                    result = xid;
                }
                xids.push(xid);
            }
            i += 1;
        }
        Ok(())
    })();
    lwlock::unlock_twophase_state::call()?;
    inner?;
    Ok((result, xids))
}

/// `StandbyRecoverPreparedTransactions` — process each prepared xact buffer with
/// `setParent = true` to update pg_subtrans.
pub fn standby_recover_prepared_transactions(
    state: &mut TwoPhaseStateData,
    orig_next_xid: TransactionId,
    transaction_xmin: TransactionId,
) -> PgResult<()> {
    lwlock::lock_twophase_state::call(true)?;
    let inner = (|| -> PgResult<()> {
        // Plain `i++` per entry, matching C's `for (i; i < numPrepXacts; i++)`:
        // a swap-removed entry leaves its replacement unscanned this pass.
        let mut i = 0;
        while i < state.num_prep_xacts() {
            let (xid, start_lsn, ondisk) = {
                let g = state.prep_xact(i);
                debug_assert!(g.inredo);
                (g.xid, g.prepare_start_lsn, g.ondisk)
            };
            let _buf =
                process_two_phase_buffer(state, xid, start_lsn, ondisk, true, false, orig_next_xid, transaction_xmin)?;
            i += 1;
        }
        Ok(())
    })();
    lwlock::unlock_twophase_state::call()?;
    inner
}

/// `RecoverPreparedTransactions` — reload the full state for each prepared xact
/// at the end of recovery. `orig_next_xid`/`in_hot_standby` are read by caller.
pub fn recover_prepared_transactions(
    state: &mut TwoPhaseStateData,
    my_locked_gxact: &mut Option<usize>,
    orig_next_xid: TransactionId,
    transaction_xmin: TransactionId,
    in_hot_standby: bool,
) -> PgResult<()> {
    lwlock::lock_twophase_state::call(true)?;
    let inner = (|| -> PgResult<()> {
        // Plain `i++` per entry per C's `for (i; i < numPrepXacts; i++)`: a
        // swap-removed entry leaves its replacement unscanned this pass.
        let mut i = 0;
        while i < state.num_prep_xacts() {
            let (xid, start_lsn, ondisk) = {
                let g = state.prep_xact(i);
                (g.xid, g.prepare_start_lsn, g.ondisk)
            };
            let buf = match process_two_phase_buffer(
                state,
                xid,
                start_lsn,
                ondisk,
                true,
                false,
                orig_next_xid,
                transaction_xmin,
            )? {
                None => {
                    i += 1;
                    continue;
                }
                Some(b) => b,
            };

            let hdr = TwoPhaseFileHeader::from_bytes(&buf).ok_or_else(|| {
                ereport(ERROR)
                    .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                    .errmsg("corrupted two-phase state buffer")
                    .into_error()
            })?;
            debug_assert_eq!(hdr.xid, xid);
            let layout = BufferLayout::of(&hdr);
            let gid = decode_gid(&buf, &layout, &hdr)?;
            let subxids = decode_children(&buf, &layout, hdr.nsubxacts as usize);

            mark_as_preparing_guts(
                state,
                i,
                my_locked_gxact,
                xid,
                &gid,
                hdr.prepared_at,
                hdr.owner,
                hdr.database,
            )?;
            state.prep_xact_mut(i).inredo = false;

            let pgprocno = state.prep_xact(i).pgprocno;
            proc::gxact_load_subxact_data::call(pgprocno, &subxids)?;
            mark_as_prepared(state, i, true)?;

            lwlock::unlock_twophase_state::call()?;

            // Recover other state (notably locks) via rmgr callbacks.
            process_records(&buf, layout.records, xid, TWOPHASE_PHASE_RECOVER)?;

            if in_hot_standby {
                standby::standby_release_lock_tree::call(xid, &subxids);
            }

            post_prepare_twophase(state, my_locked_gxact)?;

            lwlock::lock_twophase_state::call(true)?;
            i += 1;
        }
        Ok(())
    })();
    lwlock::unlock_twophase_state::call()?;
    inner
}

// ---------------------------------------------------------------------------
// CheckPointTwoPhase
// ---------------------------------------------------------------------------

/// `CheckPointTwoPhase` — fsync the state file of any gxact valid/in-redo with a
/// PREPARE LSN ≤ `redo_horizon`.
pub fn check_point_two_phase(
    state: &mut TwoPhaseStateData,
    redo_horizon: XLogRecPtr,
) -> PgResult<()> {
    if state.max_prepared_xacts == 0 {
        return Ok(());
    }

    let mut serialized_xacts = 0u32;
    lwlock::lock_twophase_state::call(false)?;
    let inner = (|| -> PgResult<()> {
        for i in 0..state.num_prep_xacts() {
            let (valid, inredo, ondisk, end_lsn, start_lsn, xid) = {
                let g = state.prep_xact(i);
                (
                    g.valid,
                    g.inredo,
                    g.ondisk,
                    g.prepare_end_lsn,
                    g.prepare_start_lsn,
                    g.xid,
                )
            };
            if (valid || inredo) && !ondisk && end_lsn <= redo_horizon {
                let buf = wal::xlog_read_twophase_data::call(start_lsn)?;
                recreate_two_phase_file(xid, &buf)?;
                let g = state.prep_xact_mut(i);
                g.ondisk = true;
                g.prepare_start_lsn = InvalidXLogRecPtr;
                g.prepare_end_lsn = InvalidXLogRecPtr;
                serialized_xacts += 1;
            }
        }
        Ok(())
    })();
    lwlock::unlock_twophase_state::call()?;
    inner?;

    files::fsync_twophase_dir::call()?;

    let _ = serialized_xacts; // C logs a summary under log_checkpoints (LOG only)
    Ok(())
}

// ---------------------------------------------------------------------------
// LookupGXact / LookupGXactBySubid / TwoPhaseTransactionGid
// ---------------------------------------------------------------------------

/// `LookupGXact` — does a valid prepared xact with this GID + origin
/// lsn/timestamp exist?
pub fn lookup_gxact(
    state: &TwoPhaseStateData,
    gid: &str,
    prepare_end_lsn: XLogRecPtr,
    origin_prepare_timestamp: TimestampTz,
) -> PgResult<bool> {
    let mut found = false;
    lwlock::lock_twophase_state::call(false)?;
    let inner = (|| -> PgResult<()> {
        for i in 0..state.num_prep_xacts() {
            let (valid, gxact_gid, ondisk, xid, start_lsn) = {
                let g = state.prep_xact(i);
                (g.valid, g.gid().to_string(), g.ondisk, g.xid, g.prepare_start_lsn)
            };
            if valid && gxact_gid == gid {
                let buf = if ondisk {
                    read_twophase_file(xid, false)?.ok_or_else(|| {
                        ereport(ERROR)
                            .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                            .errmsg("two-phase state file disappeared")
                            .into_error()
                    })?
                } else {
                    debug_assert!(start_lsn != 0);
                    wal::xlog_read_twophase_data::call(start_lsn)?
                };
                let hdr = TwoPhaseFileHeader::from_bytes(&buf).ok_or_else(|| {
                    ereport(ERROR)
                        .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                        .errmsg("corrupted two-phase state buffer")
                        .into_error()
                })?;
                if hdr.origin_lsn == prepare_end_lsn
                    && hdr.origin_timestamp == origin_prepare_timestamp
                {
                    found = true;
                    break;
                }
            }
        }
        Ok(())
    })();
    lwlock::unlock_twophase_state::call()?;
    inner?;
    Ok(found)
}

/// `TwoPhaseTransactionGid` — form the GID `pg_gid_<subid>_<xid>`.
pub fn two_phase_transaction_gid(subid: Oid, xid: TransactionId) -> PgResult<String> {
    debug_assert!(subid != 0);
    if !TransactionIdIsValid(xid) {
        return raise(ereport(ERROR)
            .errcode(types_error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg("invalid two-phase transaction ID"));
    }
    Ok(alloc::format!("pg_gid_{}_{}", subid, xid))
}

/// `IsTwoPhaseTransactionGidForSubid` — does `gid` belong to `subid`?
pub fn is_two_phase_transaction_gid_for_subid(subid: Oid, gid: &str) -> bool {
    let rest = match gid.strip_prefix("pg_gid_") {
        Some(r) => r,
        None => return false,
    };
    let mut parts = rest.splitn(2, '_');
    let subid_str = match parts.next() {
        Some(s) => s,
        None => return false,
    };
    let xid_str = match parts.next() {
        Some(s) => s,
        None => return false,
    };
    let subid_from_gid: Oid = match subid_str.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    let xid_from_gid: TransactionId = match xid_str.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    if subid != subid_from_gid {
        return false;
    }
    match two_phase_transaction_gid(subid, xid_from_gid) {
        Ok(tmp) => tmp == gid,
        Err(_) => false,
    }
}

/// `LookupGXactBySubid`.
pub fn lookup_gxact_by_subid(state: &TwoPhaseStateData, subid: Oid) -> PgResult<bool> {
    let mut found = false;
    lwlock::lock_twophase_state::call(false)?;
    for i in 0..state.num_prep_xacts() {
        let g = state.prep_xact(i);
        if g.valid && is_two_phase_transaction_gid_for_subid(subid, g.gid()) {
            found = true;
            break;
        }
    }
    lwlock::unlock_twophase_state::call()?;
    Ok(found)
}

/// `TwoPhaseFilePath` basename — `pg_twophase/%08X%08X` (epoch, xid). `epoch`
/// is `EpochFromFullTransactionId(AdjustToFullTransactionId(xid))`, read by the
/// caller off the next-full-xid it already holds.
pub fn two_phase_file_basename(epoch: u32, xid: TransactionId) -> String {
    alloc::format!("{:08X}{:08X}", epoch, xid)
}

/// `TwoPhaseFilePath` (twophase.c:945) — the full `pg_twophase/%08X%08X` path
/// for `xid`. The epoch is `EpochFromFullTransactionId(AdjustToFullTransactionId
/// (xid))`, where `AdjustToFullTransactionId` =
/// `FullTransactionIdFromAllowableAt(ReadNextFullTransactionId(), xid)`
/// (twophase.c:938). This path-format + epoch-adjustment is twophase.c's OWN
/// logic; only the raw filesystem syscalls below are delegated to fd.c.
fn two_phase_file_path(xid: TransactionId) -> String {
    let fxid = adjust_to_full_transaction_id(xid);
    alloc::format!(
        "{}/{}",
        TWOPHASE_DIR,
        two_phase_file_basename(fxid.epoch(), fxid.xid())
    )
}

/// `AdjustToFullTransactionId(xid)` / `FullTransactionIdFromAllowableAt(
/// ReadNextFullTransactionId(), xid)` (transam.h:380, twophase.c:938) — recover
/// the full xid (epoch) for a bare `xid` that is known to precede-or-equal the
/// next full xid.
fn adjust_to_full_transaction_id(xid: TransactionId) -> types_core::FullTransactionId {
    use types_core::xact::TransactionIdIsNormal;
    use types_core::FullTransactionId;

    // Special transaction ID.
    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }

    let next_full_xid = varsup::read_next_full_transaction_id::call();
    let mut epoch = next_full_xid.epoch();
    // xid must be from the epoch of nextFullXid or the epoch before.
    if xid > next_full_xid.xid() {
        debug_assert!(epoch != 0);
        epoch -= 1;
    }
    FullTransactionId::from_epoch_and_xid(epoch, xid)
}

// ---------------------------------------------------------------------------
// pg_twophase state-file raw I/O — fileio-seam bodies.
//
// These are twophase.c's own `ReadTwoPhaseFile` / `RecreateTwoPhaseFile` /
// `RemoveTwoPhaseFile` / `restoreTwoPhaseData`-scan / `CheckPointTwoPhase`-fsync
// / `PrepareRedoAdd`-probe syscall glue. The path format + epoch adjustment and
// (in the read/recreate callers above) the magic/length/CRC validation stay
// in-crate; only the raw filesystem syscalls are delegated to fd.c's installed
// primitives (`OpenTransientFile`/`CloseTransientFile`/`pg_fsync`/`fsync_fname`/
// `AllocateDir`/`ReadDir`/`FreeDir`, and the `unlink_file`/`access_f_ok` seams).
// ---------------------------------------------------------------------------

use fd as fd;
use fd_seams as fd_seams;

/// fd-access ereport at `here()` with the live errno's SQLSTATE.
fn file_access_error<T>(level: types_error::ErrorLevel, msg: String) -> PgResult<T> {
    Err(ereport(level)
        .errcode_for_file_access()
        .errmsg(msg)
        .into_error()
        .with_error_location(here()))
}

/// `ReadTwoPhaseFile`'s raw read (twophase.c:1288) — `OpenTransientFile + fstat
/// + read + close`. Returns the raw file bytes, or `None` when `missing_ok` and
/// the file does not exist (`ENOENT`).
fn seam_read_twophase_file(
    xid: TransactionId,
    missing_ok: bool,
) -> PgResult<Option<Vec<u8>>> {
    use std::io::Read;
    use std::os::fd::FromRawFd;

    let path = two_phase_file_path(xid);

    // C: fd = OpenTransientFile(path, O_RDONLY); if (fd < 0) { if (missing_ok &&
    // errno == ENOENT) return NULL; ereport(ERROR, ...). fd.c's OpenTransientFile
    // raises the open ERROR eagerly, so to honour `missing_ok` we probe for
    // existence first (access(path, F_OK)) and report any non-ENOENT errno here.
    if missing_ok {
        match fd_seams::access_f_ok::call(&path)? {
            fd_seams::AccessResult::Ok => {}
            fd_seams::AccessResult::NoEnt => return Ok(None),
            fd_seams::AccessResult::Other(_) => {
                return file_access_error(
                    ERROR,
                    alloc::format!("could not access file \"{}\": %m", path),
                );
            }
        }
    }

    let raw = fd::allocated_desc::OpenTransientFile(&path, libc::O_RDONLY)?;
    // Borrow the kernel fd as a std File without taking ownership (the close is
    // CloseTransientFile's job, mirroring C's CloseTransientFile(fd)).
    let mut file = core::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(raw) });

    let mut read_body = || -> PgResult<Vec<u8>> {
        // C: fstat(fd, &stat) for the length; here std::fs::Metadata.
        let st_size = file
            .metadata()
            .map_err(|_| {
                ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(alloc::format!("could not stat file \"{}\": %m", path))
                    .into_error()
                    .with_error_location(here())
            })?
            .len() as usize;

        // The lower/upper size bounds and all format validation live in the
        // caller (`read_twophase_file`); here we just slurp the file.
        let mut buf = alloc::vec![0u8; st_size];
        let r = file.read(&mut buf).map_err(|_| {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(alloc::format!("could not read file \"{}\": %m", path))
                .into_error()
                .with_error_location(here())
        })?;
        if r != st_size {
            return raise(ereport(ERROR).errmsg(alloc::format!(
                "could not read file \"{}\": read {} of {}",
                path,
                r,
                st_size
            )));
        }
        Ok(buf)
    };

    let result = read_body();
    // C: if (CloseTransientFile(fd) != 0) ereport(ERROR, ...). Close on every
    // path; surface a close error only when the read itself succeeded.
    let close = fd::allocated_desc::CloseTransientFile(raw);
    let buf = result?;
    close.map_err(|e| e.with_error_location(here()))?;
    Ok(Some(buf))
}

/// `RecreateTwoPhaseFile`'s store (twophase.c:1727) — `OpenTransientFile(O_CREAT
/// |O_TRUNC|O_WRONLY) + write(content) + write(crc) + pg_fsync + close`. The CRC
/// is computed in-crate by the caller (`recreate_two_phase_file`) and passed in.
fn seam_recreate_twophase_file(xid: TransactionId, content: &[u8], crc: u32) -> PgResult<()> {
    use std::io::Write;
    use std::os::fd::FromRawFd;

    let path = two_phase_file_path(xid);

    let raw = fd::allocated_desc::OpenTransientFile(
        &path,
        libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY,
    )
    .map_err(|_| {
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(alloc::format!("could not recreate file \"{}\": %m", path))
            .into_error()
            .with_error_location(here())
    })?;
    let mut file = core::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(raw) });

    let mut write_body = || -> PgResult<()> {
        // C: write(fd, content, len); write(fd, &statefile_crc, sizeof(crc)).
        file.write_all(content).map_err(|_| {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(alloc::format!("could not write file \"{}\": %m", path))
                .into_error()
                .with_error_location(here())
        })?;
        file.write_all(&crc.to_ne_bytes()).map_err(|_| {
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(alloc::format!("could not write file \"{}\": %m", path))
                .into_error()
                .with_error_location(here())
        })?;
        // C: if (pg_fsync(fd) != 0) ereport(ERROR, ...).
        fd::sync_cleanup::pg_fsync(&file).map_err(|e| e.with_error_location(here()))?;
        Ok(())
    };

    let result = write_body();
    let close = fd::allocated_desc::CloseTransientFile(raw);
    result?;
    close.map_err(|e| e.with_error_location(here()))?;
    Ok(())
}

/// `RemoveTwoPhaseFile(xid, giveWarning)` (twophase.c:1707) — `unlink(path)`;
/// a missing file warns only when `give_warning`.
fn seam_remove_twophase_file(xid: TransactionId, give_warning: bool) -> PgResult<()> {
    let path = two_phase_file_path(xid);
    let rc = fd_seams::unlink_file::call(&path);
    if rc != 0 {
        // rc is -errno; C: if (errno != ENOENT || giveWarning) ereport(WARNING).
        let errno = -rc;
        if errno != utils_error::errno::ENOENT || give_warning {
            return file_access_error(
                WARNING,
                alloc::format!("could not remove file \"{}\": %m", path),
            );
        }
    }
    Ok(())
}

/// `restoreTwoPhaseData`'s scan (twophase.c:1895) — `AllocateDir/ReadDir(
/// TWOPHASE_DIR)`, keeping the 16-hex-char basenames decoded to their full-xid
/// `u64` values.
fn seam_scan_twophase_dir() -> PgResult<Vec<u64>> {
    let mut out: Vec<u64> = Vec::new();
    fd::allocated_desc::with_allocated_dir(TWOPHASE_DIR, &mut |name: &str| {
        // C: strlen == 16 && strspn(name, "0123456789ABCDEF") == 16.
        if name.len() == 16 && name.bytes().all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
        {
            // C: FullTransactionIdFromU64(strtou64(name, NULL, 16)).
            if let Ok(v) = u64::from_str_radix(name, 16) {
                out.push(v);
            }
        }
        Ok(false)
    })?;
    Ok(out)
}

/// `CheckPointTwoPhase`'s `fsync_fname(TWOPHASE_DIR, true)` (twophase.c:1866).
fn seam_fsync_twophase_dir() -> PgResult<()> {
    fd::sync_cleanup::fsync_fname(TWOPHASE_DIR, true).map_err(|e| e.with_error_location(here()))
}

/// `PrepareRedoAdd`'s `access(TwoPhaseFilePath(xid), F_OK)` probe
/// (twophase.c:2509). `Ok(true)` if it exists, `Ok(false)` on `ENOENT`, `Err`
/// for any other errno.
fn seam_twophase_file_exists(xid: TransactionId) -> PgResult<bool> {
    let path = two_phase_file_path(xid);
    match fd_seams::access_f_ok::call(&path)? {
        fd_seams::AccessResult::Ok => Ok(true),
        fd_seams::AccessResult::NoEnt => Ok(false),
        fd_seams::AccessResult::Other(_) => {
            file_access_error(ERROR, alloc::format!("could not access file \"{}\": %m", path))
        }
    }
}

// ---------------------------------------------------------------------------
// Exit-hook registration (AtProcExit_Twophase / before_shmem_exit)
// ---------------------------------------------------------------------------

/// `before_shmem_exit(AtProcExit_Twophase, 0)` — register the exit hook on
/// first use (`twophaseExitRegistered` tracks the once-only guard in the
/// caller). `AtProcExit_Twophase` runs the same cleanup as `AtAbort_Twophase`;
/// because the locked-gxact bookkeeping is backend-private state the abort path
/// owns, the registered hook delegates to the backend-installed cleanup slot
/// (set via [`set_proc_exit_cleanup`]); it is a no-op until the owning backend
/// state installs one. This is the exit safety-net, not a release registry.
fn register_twophase_exit() -> PgResult<()> {
    ipc::before_shmem_exit::call(at_proc_exit_twophase, types_tuple::Datum::from_i32(0))
}

/// `AtProcExit_Twophase(code, arg)` — exit hook; defers to the installed
/// cleanup (same logic as `AtAbort_Twophase`).
fn at_proc_exit_twophase(_code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    PROC_EXIT_CLEANUP.with(|c| match &*c.borrow() {
        Some(f) => f(),
        None => Ok(()),
    })
}

thread_local! {
    /// The backend-installed `AtAbort_Twophase` cleanup, run by the exit hook.
    static PROC_EXIT_CLEANUP: core::cell::RefCell<Option<alloc::boxed::Box<dyn Fn() -> PgResult<()>>>> =
        const { core::cell::RefCell::new(None) };
}

/// Install the exit-time cleanup the backend wants `AtProcExit_Twophase` to run
/// (its `AtAbort_Twophase` over the live 2PC state). Backend-private.
pub fn set_proc_exit_cleanup(f: alloc::boxed::Box<dyn Fn() -> PgResult<()>>) {
    PROC_EXIT_CLEANUP.with(|c| *c.borrow_mut() = Some(f));
}

// ---------------------------------------------------------------------------
// Shared-memory substrate — the process-global `TwoPhaseState`
// ---------------------------------------------------------------------------
//
// C keeps `TwoPhaseStateData *TwoPhaseState` in main shared memory, stood up by
// `TwoPhaseShmemInit` (`ShmemInitStruct`) and reached by EVERY backend at the
// same mapped address. This port places the genuinely-shared state at that real
// `ShmemInitStruct` address: [`TWO_PHASE_STATE_BASE`] holds the shared base
// pointer (set once by `TwoPhaseShmemInit` in the postmaster, before fork, so
// every forked backend inherits the identical mapping at the same VA — exactly
// like ProcSignal/MainLWLockArray). `with_twophase_state` builds a fresh
// [`TwoPhaseStateData`] handle over that base on each call, so a prepared
// transaction created by one backend is visible to any other backend (including
// a fresh connection after `\c -`). `TwoPhaseStateLock` (a real cross-process
// LWLock) still serializes mutation via the lock-guard seams.

/// Base of the genuinely-shared `TwoPhaseStateData` block (the C
/// `TwoPhaseState` pointer). Set by [`two_phase_shmem_init`] in the postmaster;
/// inherited by every forked backend (same MAP_SHARED VA).
static TWO_PHASE_STATE_BASE: std::sync::atomic::AtomicPtr<u8> =
    std::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

/// Cached `max_prepared_transactions` (set alongside the base at shmem init).
static TWO_PHASE_MAX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// `TwoPhaseShmemSize()` (seam-facing) — mirrors C's `TwoPhaseShmemSize(void)`,
/// which reads the `max_prepared_xacts` GUC global itself. The zero-arg
/// inward seam reads the owner's per-backend GUC here (the ipci consumer has
/// no value to thread, exactly as C's `add_size(size, TwoPhaseShmemSize())`).
fn two_phase_shmem_size_seam() -> usize {
    two_phase_shmem_size(max_prepared_xacts_guc())
}

/// `TwoPhaseShmemInit()` — allocate-or-attach the global `TwoPhaseState` in
/// main shared memory and (non-`IsUnderPostmaster` path) build the GXACT
/// freelist over the preallocated dummy PGPROCs. The real bytes are reserved
/// via the `ShmemInitStruct` seam (whose `found` mirrors C's `*foundPtr`); the
/// owned [`TwoPhaseStateData`] model is then materialized over the GUC-sized
/// freelist. Idempotent per process (the `OnceLock` matches the C invariant
/// that `TwoPhaseShmemInit` runs once at shmem creation).
pub fn two_phase_shmem_init() -> PgResult<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let max_prepared_xacts = max_prepared_xacts_guc();
    // Reserve/attach the shared allocation (the C `ShmemInitStruct`); `found`
    // is the C `*foundPtr` — true when attaching to an already-created segment.
    let (base, found) = ipc_shmem::shmem_init_struct::call(
        "Prepared Transaction Table",
        two_phase_shmem_size(max_prepared_xacts),
    )?;
    TWO_PHASE_STATE_BASE.store(base, Relaxed);
    TWO_PHASE_MAX.store(max_prepared_xacts, Relaxed);
    // Only the creator (C `!found` / `!IsUnderPostmaster`) initializes the
    // freelist; an attaching backend leaves the shared bytes untouched.
    if !found {
        let mut state = TwoPhaseStateData { base, max_prepared_xacts };
        state.init_shared();
    }
    Ok(())
}

/// Run `f` with `&mut TwoPhaseStateData` over the genuinely-shared 2PC state.
/// The handle is a thin wrapper over the `ShmemInitStruct` base, so every call
/// (in any backend) reaches the SAME shared bytes — no per-process copy. C
/// serializes mutation with the cross-process `TwoPhaseStateLock` LWLock, which
/// the lock-guard seams inside the algorithmic functions still take; the
/// `&mut TwoPhaseStateData` here is a per-call view, not the synchronization
/// primitive. Reentrant 2PC paths (e.g. `FinishPreparedTransaction` holding the
/// lock across callbacks that call `TwoPhaseGetDummyProc(lock_held=true)`) are
/// fine: each builds its own handle over the same shmem, and the LWLock's
/// `lock_held` parameter governs re-entry exactly as in C.
///
/// Panics if [`two_phase_shmem_init`] has not run (the C invariant: the shmem
/// struct exists before any prepare/abort/redo path touches it).
pub fn with_twophase_state<R>(f: impl FnOnce(&mut TwoPhaseStateData) -> R) -> R {
    use std::sync::atomic::Ordering::Relaxed;
    let base = TWO_PHASE_STATE_BASE.load(Relaxed);
    assert!(
        !base.is_null(),
        "TwoPhaseState accessed before TwoPhaseShmemInit"
    );
    let mut state = TwoPhaseStateData {
        base,
        max_prepared_xacts: TWO_PHASE_MAX.load(Relaxed),
    };
    f(&mut state)
}

// ---------------------------------------------------------------------------
// Per-backend bookkeeping (C file-scope statics: MyLockedGxact,
// twophaseExitRegistered)
// ---------------------------------------------------------------------------

thread_local! {
    /// `MyLockedGxact` (twophase.c) — the prepXacts index this backend has
    /// locked while preparing/finishing, threaded into the algorithmic
    /// functions and cleared by post-prepare / abort.
    static MY_LOCKED_GXACT: core::cell::RefCell<Option<usize>> =
        const { core::cell::RefCell::new(None) };

    /// `twophaseExitRegistered` (twophase.c) — once-only guard for the
    /// `before_shmem_exit(AtProcExit_Twophase)` registration.
    static TWOPHASE_EXIT_REGISTERED: core::cell::RefCell<bool> =
        const { core::cell::RefCell::new(false) };

    /// `reachedConsistency` (xlogrecovery.c) — the recovery-consistency flag
    /// `PrepareRedoAdd` consults to choose ERROR vs WARNING on a duplicate
    /// 2PC file. Backend-private; the startup/redo path sets it (defaults to
    /// false, i.e. pre-consistency early recovery).
    static REACHED_CONSISTENCY: core::cell::RefCell<bool> =
        const { core::cell::RefCell::new(false) };

    /// The in-flight 2PC state-file builder between this backend's
    /// `StartPrepare` and `EndPrepare` (C's file-scope `records` workspace).
    static PREPARE_BUILDER: core::cell::RefCell<Option<SaveState>> =
        const { core::cell::RefCell::new(None) };
}

/// Set `reachedConsistency` for the install of [`prepare_redo_add`] (called by
/// the WAL-recovery driver when consistency is reached).
pub fn set_reached_consistency(v: bool) {
    REACHED_CONSISTENCY.with(|c| *c.borrow_mut() = v);
}

// ---------------------------------------------------------------------------
// Seam install
// ---------------------------------------------------------------------------

/// Install this crate's inward seams (`backend-access-transam-twophase-seams`).
pub fn init_seams() {
    use twophase_seams as seams;

    // `max_prepared_xacts` GUC (twophase.c:115). Install the accessor bridging the
    // C-global storage so the shmem-sizing path (procarray) can read it.
    guc_tables::vars::max_prepared_xacts.install(
        guc_tables::GucVarAccessors {
            get: || max_prepared_xacts_guc() as i32,
            set: |v| set_max_prepared_xacts(v.max(0) as usize),
        },
    );

    seams::standby_transaction_id_is_prepared::set(|xid| {
        standby_transaction_id_is_prepared(xid, max_prepared_xacts_guc())
    });

    // `XlogReadTwoPhaseData` (twophase.c:1404) — re-read a prepare record from
    // WAL. The seam is homed in xlog-seams (sibling-file split) but the body
    // belongs to twophase.c, so this unit owns its install.
    wal::xlog_read_twophase_data::set(xlog_read_twophase_data);

    // `MarkAsPreparing` — over the global state + this backend's
    // MyLockedGxact / twophaseExitRegistered statics. The returned slot is
    // stashed in MyLockedGxact for the matching StartPrepare/EndPrepare.
    seams::mark_as_preparing::set(|xid, gid, prepared_at, owner, databaseid| {
        with_twophase_state(|state| {
            MY_LOCKED_GXACT.with(|locked| {
                TWOPHASE_EXIT_REGISTERED.with(|reg| {
                    mark_as_preparing(
                        state,
                        &mut reg.borrow_mut(),
                        &mut locked.borrow_mut(),
                        xid,
                        gid,
                        prepared_at,
                        owner,
                        databaseid,
                    )
                    .map(|_slot| ())
                })
            })
        })
    });

    // `StartPrepare` — build the 2PC state-file workspace from the gathered
    // args over the locked gxact's dummy PGPROC; stash the builder.
    seams::start_prepare::set(|args| {
        let pgprocno = with_twophase_state(|state| {
            let slot = my_locked_gxact_slot();
            state.prep_xact(slot).pgprocno
        });
        let input = StartPrepareInput {
            xid: args.xid,
            gid: &args.gid,
            prepared_at: args.prepared_at,
            owner: args.owner,
            databaseid: args.databaseid,
            children: &args.children,
            commitrels: &args.commitrels,
            ncommitrels: args.ncommitrels,
            abortrels: &args.abortrels,
            nabortrels: args.nabortrels,
            commitstats: &args.commitstats,
            ncommitstats: args.ncommitstats,
            abortstats: &args.abortstats,
            nabortstats: args.nabortstats,
            invalmsgs: &args.invalmsgs,
            ninvalmsgs: args.ninvalmsgs,
            initfileinval: args.initfileinval,
        };
        let builder = start_prepare(&input, pgprocno)?;
        PREPARE_BUILDER.with(|b| *b.borrow_mut() = Some(builder));
        Ok(())
    });

    // `EndPrepare` — finish the stashed builder for the locked gxact, reading
    // the ambient replication-origin session via the origin seams.
    seams::end_prepare::set(|| {
        let builder = PREPARE_BUILDER
            .with(|b| b.borrow_mut().take())
            .expect("EndPrepare called without a matching StartPrepare builder");
        let repl = ReplOriginSession {
            origin: origin::replorigin_session_origin::call(),
            origin_lsn: origin::replorigin_session_origin_lsn::call(),
            origin_timestamp: origin::replorigin_session_origin_timestamp::call(),
        };
        let slot = my_locked_gxact_slot();
        with_twophase_state(|state| end_prepare(state, slot, builder, repl))
    });

    // `RegisterTwoPhaseRecord` — append an RM's 2PC record to the in-flight
    // prepare builder StartPrepare stashed (C's file-scope `records`).
    seams::register_two_phase_record::set(|rmid, info, data| {
        PREPARE_BUILDER.with(|b| {
            let mut slot = b.borrow_mut();
            let builder = slot
                .as_mut()
                .expect("RegisterTwoPhaseRecord called without a matching StartPrepare builder");
            register_two_phase_record(builder, rmid, info, data)
        })
    });

    // `TwoPhaseGetDummyProcNumber` — the dummy PGPROC standing in for a
    // prepared transaction's xid.
    seams::two_phase_get_dummy_proc_number::set(|xid, lock_held| {
        with_twophase_state(|state| two_phase_get_dummy_proc_number(state, xid, lock_held))
    });

    seams::two_phase_get_xid_by_virtual_xid::set(|vxid| {
        with_twophase_state(|state| {
            let mut have_more = false;
            let xid = two_phase_get_xid_by_virtual_xid(state, vxid, &mut have_more)?;
            Ok((xid, have_more))
        })
    });

    // `PostPrepare_Twophase` / `AtAbort_Twophase` — void cleanup in C (no
    // ereport); the PgResult is from the lock seams / Assert-equivalent
    // RemoveGXact, surfaced as a panic on the impossible-error path.
    seams::post_prepare_twophase::set(|| {
        with_twophase_state(|state| {
            MY_LOCKED_GXACT.with(|locked| {
                post_prepare_twophase(state, &mut locked.borrow_mut())
            })
        })
        .expect("PostPrepare_Twophase failed")
    });

    seams::at_abort_twophase::set(|| {
        with_twophase_state(|state| {
            MY_LOCKED_GXACT.with(|locked| at_abort_twophase(state, &mut locked.borrow_mut()))
        })
        .expect("AtAbort_Twophase failed")
    });

    // `PrepareRedoAdd` / `PrepareRedoRemove` — recovery paths, under
    // TwoPhaseStateLock (the lock-guard seams bracket inside the fns).
    seams::prepare_redo_add::set(|data, start_lsn, end_lsn, origin_id| {
        let reached = REACHED_CONSISTENCY.with(|c| *c.borrow());
        with_twophase_state(|state| {
            prepare_redo_add(state, data, start_lsn, end_lsn, origin_id, reached)
        })
    });

    seams::prepare_redo_remove::set(|xid, give_warning| {
        with_twophase_state(|state| prepare_redo_remove(state, xid, give_warning))
    });

    // `TwoPhaseShmemSize` / `TwoPhaseShmemInit` — shmem sizing/stand-up
    // (consumed by ipci.c when it lands).
    seams::two_phase_shmem_size::set(two_phase_shmem_size_seam);
    seams::two_phase_shmem_init::set(two_phase_shmem_init);

    // pg_twophase state-file raw I/O (`backend-access-transam-twophase-fileio-
    // seams`). These are twophase.c's own file helpers; the path-format/CRC/
    // scan logic is in-crate and only the raw syscalls are seamed to fd.c.
    files::read_twophase_file::set(seam_read_twophase_file);
    files::recreate_twophase_file::set(seam_recreate_twophase_file);
    files::remove_twophase_file::set(seam_remove_twophase_file);
    files::scan_twophase_dir::set(seam_scan_twophase_dir);
    files::fsync_twophase_dir::set(seam_fsync_twophase_dir);
    files::twophase_file_exists::set(seam_twophase_file_exists);

    // WAL-startup entry points called by `StartupXLOG` (xlog.c) on the clean
    // DB_SHUTDOWNED / end-of-recovery path. `orig_next_xid` (TransamVariables->
    // nextXid) and `transaction_xmin` (TransactionXmin) are globals in C; the
    // WAL-startup driver reads them from their owners and threads them in. The
    // owner wraps its private `TwoPhaseStateData` (and the `MY_LOCKED_GXACT`
    // thread-local for the recover path) through the ambient accessor.
    seams::restore_two_phase_data::set(|orig_next_xid, transaction_xmin, reached_consistency| {
        with_twophase_state(|state| {
            restore_two_phase_data(state, orig_next_xid, transaction_xmin, reached_consistency)
        })
    });
    seams::prescan_prepared_transactions::set(|orig_next_xid, transaction_xmin| {
        with_twophase_state(|state| {
            prescan_prepared_transactions(state, orig_next_xid, transaction_xmin)
                .map(|(oldest_active_xid, _xids)| oldest_active_xid)
        })
    });
    seams::standby_recover_prepared_transactions::set(|orig_next_xid, transaction_xmin| {
        with_twophase_state(|state| {
            standby_recover_prepared_transactions(state, orig_next_xid, transaction_xmin)
        })
    });
    seams::recover_prepared_transactions::set(|orig_next_xid, transaction_xmin, in_hot_standby| {
        with_twophase_state(|state| {
            MY_LOCKED_GXACT.with(|locked| {
                recover_prepared_transactions(
                    state,
                    &mut locked.borrow_mut(),
                    orig_next_xid,
                    transaction_xmin,
                    in_hot_standby,
                )
            })
        })
    });

    // `FinishPreparedTransaction(gid, isCommit)` — the utility dispatch
    // (`standard_ProcessUtility`'s COMMIT/ROLLBACK PREPARED arms) reaches this
    // twophase.c entry point through `backend-tcop-utility-out-seams`. The
    // function lives here; the adapter snapshots the ambient backend state C's
    // `FinishPreparedTransaction` reads off its own globals (GetUserId,
    // MyDatabaseId, the replication-origin session, GetCurrentTimestamp,
    // TransactionXmin, MyXactFlags, MyDatabaseTableSpace, XLogLogicalInfoActive)
    // and threads it as the explicit `FinishContext`, then runs the algorithm
    // over the process-global `TwoPhaseState` and this backend's
    // MyLockedGxact / twophaseExitRegistered statics.
    utility_out_seams::finish_prepared_transaction::set(
        |gid, is_commit| -> PgResult<()> {
            let gid = gid.expect("COMMIT/ROLLBACK PREPARED requires a transaction id");
            let ctx = FinishContext {
                user_id: miscinit::get_user_id::call(),
                my_database_id: init_small::my_database_id::call(),
                repl: ReplOriginSession {
                    origin: origin::replorigin_session_origin::call(),
                    origin_lsn: origin::replorigin_session_origin_lsn::call(),
                    origin_timestamp: origin::replorigin_session_origin_timestamp::call(),
                },
                current_timestamp: timestamp::get_current_timestamp::call(),
                transaction_xmin: snapmgr::transaction_xmin::call()?,
                my_xact_flags: xact::my_xact_flags::call(),
                my_database_table_space: init_small::my_database_table_space::call(),
                xlog_logical_info_active: wal::xlog_logical_info_active::call(),
            };
            with_twophase_state(|state| {
                MY_LOCKED_GXACT.with(|locked| {
                    TWOPHASE_EXIT_REGISTERED.with(|reg| {
                        finish_prepared_transaction(
                            state,
                            &mut reg.borrow_mut(),
                            &mut locked.borrow_mut(),
                            gid,
                            is_commit,
                            ctx,
                        )
                    })
                })
            })
        },
    );
}

/// Read this backend's `MyLockedGxact` slot, panicking if unset (the prepare
/// flow always marks-as-preparing before start/end).
fn my_locked_gxact_slot() -> usize {
    MY_LOCKED_GXACT
        .with(|c| *c.borrow())
        .expect("no MyLockedGxact set for the in-flight prepare")
}

/// `max_prepared_xacts` GUC, a per-backend value (thread_local). The inward
/// seam reads it here because the standby caller has no state handle to pass.
fn max_prepared_xacts_guc() -> usize {
    MAX_PREPARED_XACTS.with(|c| *c.borrow())
}

thread_local! {
    /// `max_prepared_xacts` GUC (backend-private; assigned at startup from the
    /// guc machinery when it lands).
    static MAX_PREPARED_XACTS: core::cell::RefCell<usize> = const { core::cell::RefCell::new(0) };
}

/// Set the backend's `max_prepared_xacts` GUC (called by the guc assign hook).
pub fn set_max_prepared_xacts(v: usize) {
    MAX_PREPARED_XACTS.with(|c| *c.borrow_mut() = v);
}
