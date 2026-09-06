//! C: src/backend/backup/basebackup_incremental.c (PG 18.3) — support code
//! for incremental base backups: the `IncrementalBackupInfo` built from the
//! manifest a client supplies via UPLOAD_MANIFEST, and
//! `PrepareForIncrementalBackup`, which cross-checks the summarized-WAL
//! coverage and determines the LSN from which the incremental is taken.
//!
//! Full scope (incremental-basebackup-port.md): CreateIncrementalBackupInfo,
//! AppendIncrementalManifestData, FinalizeIncrementalManifest,
//! PrepareForIncrementalBackup, plus the send-side consumers
//! (GetFileBackupMethod, GetIncrementalFilePath,
//! GetIncrementalHeaderSize/GetIncrementalFileSize) that the basebackup
//! sendDir/sendFile path calls per relation file.
//!
//! Divergences from C, all deliberate:
//!
//! - **Ownership (Q2 ruling, binding).** C stores the parsed manifest in a
//!   per-process static parented under CacheMemoryContext (walsender.c:152).
//!   pgrust is thread-per-backend heading to pooled sessions ("no state
//!   belongs to a thread"), so this object — potentially 100s of MB — is
//!   plain owned data with no static, no thread-local, and no memory-context
//!   parentage; the walsender crate stores it in its *session-slot*-keyed
//!   store and drops it when the session releases the slot. See
//!   `walsender::uploaded_manifest` for the ownership site.
//! - **Parse timing.** C parses incrementally during upload
//!   (json_parse_manifest_incremental_*, MIN_CHUNK/MAX_CHUNK buffering so the
//!   checksum line lands in the is_last call). pgrust's Stage-2
//!   `parse_manifest` is whole-buffer only, so `AppendIncrementalManifestData`
//!   accumulates verbatim and `FinalizeIncrementalManifest` runs one
//!   whole-buffer parse. The checksum covers the same bytes and every error
//!   string is unchanged, so the difference is not client-visible. Because
//!   the whole manifest is retained (no incremental parser to drain it), the
//!   accumulation buffer is bounded to MAX_ALLOC_SIZE (1 GB - 1) with fallible
//!   allocation — the same ceiling and catchable-ERROR behavior C's StringInfo
//!   gives every buffer — so a replication client cannot stream an unbounded
//!   run of CopyData packets to exhaust server memory or abort the process.
//! - **brtab lifetime.** C stashes the merged block-reference table in
//!   `ib->brtab` for GetFileBackupMethod. Here `PrepareForIncrementalBackup`
//!   *returns* the `BlockRefTable`, tied to the caller's (command-lifetime)
//!   memory context — the basebackup send path threads it to
//!   GetFileBackupMethod alongside
//!   `&IncrementalBackupInfo`. This avoids a self-referential
//!   session-object/arena pairing (the C1 decode-arena lifetime trap).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::collections::HashMap;

use blkreftable::{BlockRefTable, BlockRefTableReader};
use elog::ereport;
use manifest::checksum::PgChecksumType;
use mcx::{oom_named, Mcx, MemoryContext, PgVec, MAX_ALLOC_SIZE};
use parse_manifest::{json_parse_manifest, JsonManifestParseContext};
use timeline_seams::TimeLineHistoryEntry;
use types_core::{
    BlockNumber, ForkNumber, InvalidBlockNumber, Oid, RelFileNumber, TimeLineID, XLogRecPtr,
    BLCKSZ,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_INTERNAL_ERROR,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use types_storage::RelFileLocator;
use walsummarizer::{
    FilterWalSummaries, GetWalSummaries, WaitForWalSummarization, WalSummariesAreComplete,
    WalSummaryFile,
};
use xlogbackup::BackupState;

const InvalidXLogRecPtr: XLogRecPtr = 0;

/// C: BLOCKS_PER_READ — batch size for draining a summary reader.
const BLOCKS_PER_READ: usize = 512;

// wait_event_names.txt IO section: WalSummaryRead (pinned by
// scripts/lint-waitevent-tags.sh against the waitevent name table).
const WAIT_EVENT_WAL_SUMMARY_READ: u32 = 0x0A00_0000 + 76;

/// C: XLOGDIR "/summaries" (the production summary directory; tests point
/// the merge helper at a scratch directory instead).
pub const WAL_SUMMARIES_DIR: &str = "pg_wal/summaries";

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    // C: LSN_FORMAT_ARGS under "%X/%X".
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// C: backup_wal_range — details extracted from the WAL ranges present in
/// the supplied backup manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupWalRange {
    pub tli: TimeLineID,
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
}

/// C: struct IncrementalBackupInfo (backup_file_entry hash + WAL-range list
/// + manifest accumulation buffer). Plain owned data — see the module
/// comment for why there is no memory context and no static here.
pub struct IncrementalBackupInfo {
    /// This server's system identifier, captured at creation time. C reads
    /// GetSystemIdentifier() inside the manifest callback; capturing it at
    /// CreateIncrementalBackupInfo time is value-identical (the sysid is
    /// immutable for the life of a cluster) and keeps this crate testable.
    system_identifier: u64,

    /// C: ib->buf — temporary buffer storing the manifest while parsing.
    /// Bounded to MAX_ALLOC_SIZE with fallible growth in
    /// AppendIncrementalManifestData (C: StringInfo's MaxAllocSize ceiling).
    buf: Vec<u8>,
    /// True once FinalizeIncrementalManifest has run (C: buf.data == NULL).
    finalized: bool,

    /// C: ib->manifest_files (backup_file_hash) — path -> size. Retained for
    /// sanity checking only; checksums and mtimes are deliberately dropped
    /// (C keeps names and sizes, nothing else). Lookup-only: never iterated
    /// on any output path (map order must stay non-surface).
    manifest_files: HashMap<Box<[u8]>, u64>,

    /// C: ib->manifest_wal_ranges, in manifest order.
    manifest_wal_ranges: Vec<BackupWalRange>,
}

/// C: CreateIncrementalBackupInfo(mcxt). No memory-context argument: the
/// object owns its allocations and the *holder* (the walsender session slot)
/// defines the lifetime.
pub fn CreateIncrementalBackupInfo(system_identifier: u64) -> IncrementalBackupInfo {
    IncrementalBackupInfo {
        system_identifier,
        buf: Vec::new(),
        finalized: false,
        // C pre-sizes backup_file_create(mcxt, 10000, NULL) — "a fresh initdb
        // creates almost 1000 files ... substantially higher".
        manifest_files: HashMap::with_capacity(10000),
        manifest_wal_ranges: Vec::new(),
    }
}

/// The MaxAllocSize admission check for the manifest accumulation buffer,
/// split out so the bound is unit-testable without allocating ~1 GB. `len` is
/// the bytes already buffered, `needed` the size of the incoming chunk.
///
/// C: enlargeStringInfo (stringinfo.c:357) — `appendBinaryStringInfo` on
/// ib->buf rejects `needed >= MaxAllocSize - str->len` (the buffer keeps one
/// byte for its terminating NUL) with ERRCODE_PROGRAM_LIMIT_EXCEEDED and the
/// StringInfo message + detail, which are what a replication client sees.
/// (`len` is kept < MAX_ALLOC_SIZE by this very check, so `saturating_sub`
/// never actually saturates — it is belt-and-suspenders.)
fn check_manifest_capacity(len: usize, needed: usize) -> PgResult<()> {
    if needed >= MAX_ALLOC_SIZE.saturating_sub(len) {
        return Err(elog::PgError::error(format!(
            "string buffer exceeds maximum allowed length ({MAX_ALLOC_SIZE} bytes)"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_detail(format!(
            "Cannot enlarge string buffer containing {len} bytes by {needed} more bytes."
        ))
        .into());
    }
    Ok(())
}

impl IncrementalBackupInfo {
    /// C: AppendIncrementalManifestData — each chunk of manifest data
    /// received from the client is passed here. C interleaves incremental
    /// parsing (MIN_CHUNK/MAX_CHUNK) so its staging StringInfo never holds
    /// more than ~MAX_CHUNK at once; pgrust's Stage-2 `parse_manifest` is
    /// whole-buffer only, so this accumulates verbatim and parses in
    /// FinalizeIncrementalManifest (see module comment).
    ///
    /// Because the whole manifest is retained here rather than streamed
    /// through an incremental parser, the buffer is a single logical
    /// allocation and is held to the same ceiling every C StringInfo
    /// enforces — MAX_ALLOC_SIZE (1 GB - 1). A replication client that
    /// streams an unbounded run of CopyData packets is rejected with a
    /// catchable ERRCODE_PROGRAM_LIMIT_EXCEEDED carrying C's
    /// enlargeStringInfo overflow ERROR text, instead of driving the process
    /// into an infallible-allocation abort. Growth uses `try_reserve`, so an
    /// allocator failure short of the cap also surfaces as a per-command
    /// out-of-memory ERROR (C: palloc failure is a catchable ERROR, not a
    /// crash) rather than aborting the whole thread-per-backend server.
    pub fn AppendIncrementalManifestData(&mut self, data: &[u8]) -> PgResult<()> {
        debug_assert!(!self.finalized, "append after FinalizeIncrementalManifest");

        let len = self.buf.len();
        let needed = data.len();

        // C: enlargeStringInfo caps any single buffer at MaxAllocSize and
        // raises ERRCODE_PROGRAM_LIMIT_EXCEEDED. Mirror that ceiling here so
        // client-controlled accumulation cannot grow without bound.
        check_manifest_capacity(len, needed)?;

        // Fallible allocation: an allocator failure becomes a catchable
        // per-command ERROR instead of handle_alloc_error aborting the
        // process (C: palloc failure raises ERROR).
        self.buf
            .try_reserve(needed)
            .map_err(|_| oom_named("incremental backup manifest", len + needed))?;
        self.buf.extend_from_slice(data);
        Ok(())
    }

    /// C: FinalizeIncrementalManifest — parse the manifest (the whole text,
    /// here) and release the accumulation buffer.
    pub fn FinalizeIncrementalManifest(&mut self) -> PgResult<()> {
        // Short-lived context backing the JSON lexer's de-escaped tokens;
        // dropped as soon as the parse completes (C: pfree of ib->buf plus
        // the incremental-state shutdown).
        let parse_cx = MemoryContext::new("incremental backup manifest parse");
        let buf = std::mem::take(&mut self.buf);
        let result = {
            let mut ctx = IbManifestContext { ib: self };
            json_parse_manifest(parse_cx.mcx(), &mut ctx, &buf)
        };
        // C frees ib->buf unconditionally on the success path only (error
        // paths abort the command and drop the whole object); mirror that —
        // on error the caller discards this IncrementalBackupInfo.
        result?;
        self.finalized = true;
        Ok(())
    }

    /// The number of file entries taken from the manifest
    /// (GetFileBackupMethod consumes the lookups; tests consume the count).
    pub fn manifest_file_count(&self) -> usize {
        self.manifest_files.len()
    }

    /// C: backup_file_lookup(ib->manifest_files, path) != NULL.
    pub fn manifest_file_lookup(&self, path: &[u8]) -> Option<u64> {
        self.manifest_files.get(path).copied()
    }

    pub fn manifest_wal_ranges(&self) -> &[BackupWalRange] {
        &self.manifest_wal_ranges
    }

    /// C: PrepareForIncrementalBackup(ib, backup_state).
    ///
    /// Performs sanity checks on the data extracted from the manifest,
    /// figures out for which WAL ranges we need summaries and whether those
    /// summaries are available, then reads and combines the data from the
    /// summary files. Updates `backup_state` with the reference TLI and LSN
    /// for the prior backup (istarttli/istartpoint).
    ///
    /// Returns the merged block-reference table (C: ib->brtab), allocated in
    /// `mcx` — the caller's command-lifetime context (see module comment).
    pub fn PrepareForIncrementalBackup<'mcx>(
        &mut self,
        mcx: Mcx<'mcx>,
        backup_state: &mut BackupState,
    ) -> PgResult<BlockRefTable<'mcx>> {
        // C: Assert(ib->buf.data == NULL) — manifest must be finalized.
        assert!(self.finalized, "PrepareForIncrementalBackup before FinalizeIncrementalManifest");

        // Match the manifest's WAL ranges against this server's timeline
        // history (C's readTimeLineHistory of the backup-start TLI).
        let expected_tles = timeline_seams::read_timeline_history::call(mcx, backup_state.starttli)?;

        let (earliest_wal_range_tli, earliest_wal_range_start_lsn) =
            self.validate_wal_ranges(&expected_tles, backup_state)?;

        // Wait for WAL summarization to catch up to the backup start LSN
        // (Q4 ruling: pinning the worker for up to minutes is accepted; this
        // is already a long-pinning walsender operation). Throws if the
        // summarizer appears stuck; returns immediately if summarize_wal is
        // off, in which case the completeness check below errors out.
        WaitForWalSummarization(backup_state.startpoint)?;

        // Retrieve all WAL summaries on any timeline that overlap the LSN
        // range of interest, in one directory read.
        let all_wslist = GetWalSummaries(
            mcx,
            0,
            earliest_wal_range_start_lsn,
            backup_state.startpoint,
        )?;

        let required_wslist = collect_required_summaries(
            mcx,
            &expected_tles,
            &all_wslist,
            backup_state.starttli,
            backup_state.startpoint,
            earliest_wal_range_tli,
            earliest_wal_range_start_lsn,
        )?;

        // Read all required block reference table files and merge them into
        // a single in-memory table.
        merge_required_summaries(mcx, &required_wslist, WAL_SUMMARIES_DIR)
    }

    /// The two WAL-range loops of C's PrepareForIncrementalBackup: match
    /// every manifest WAL range to a TimeLineHistoryEntry, decide the
    /// earliest/latest ranges, propagate istartpoint/istarttli, and sanity
    /// check every range's start/end LSN. Split out (pure over
    /// `expected_tles`) for unit testing; the shape is verbatim C.
    fn validate_wal_ranges(
        &self,
        expected_tles: &[TimeLineHistoryEntry],
        backup_state: &mut BackupState,
    ) -> PgResult<(TimeLineID, XLogRecPtr)> {
        let num_wal_ranges = self.manifest_wal_ranges.len();

        // A valid backup manifest must always contain at least one WAL range
        // (usually exactly one, unless the backup spanned a timeline switch).
        if num_wal_ranges == 0 {
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("manifest contains no required WAL ranges")
                .finish(loc("PrepareForIncrementalBackup"))?;
            unreachable!();
        }

        let mut earliest_wal_range_tli: TimeLineID = 0;
        let mut earliest_wal_range_start_lsn: XLogRecPtr = InvalidXLogRecPtr;
        let mut latest_wal_range_tli: TimeLineID = 0;

        // Note that readTimeLineHistory puts the latest timeline at the
        // beginning of the list. Hence the earliest TLI is the one nearest
        // the END of expected_tles, and the latest is nearest the beginning.
        let mut tlep: Vec<Option<&TimeLineHistoryEntry>> = vec![None; num_wal_ranges];
        for (i, range) in self.manifest_wal_ranges.iter().enumerate() {
            let mut saw_earliest_wal_range_tli = false;
            let mut saw_latest_wal_range_tli = false;

            // Search this server's history for this WAL range's TLI.
            for tle in expected_tles {
                if tle.tli == range.tli {
                    tlep[i] = Some(tle);
                    break;
                }
                if tle.tli == earliest_wal_range_tli {
                    saw_earliest_wal_range_tli = true;
                }
                if tle.tli == latest_wal_range_tli {
                    saw_latest_wal_range_tli = true;
                }
            }

            // An incremental backup can only be taken relative to a backup
            // that represents a previous state of this server.
            if tlep[i].is_none() {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "timeline {} found in manifest, but not in this server's history",
                        range.tli
                    ))
                    .finish(loc("PrepareForIncrementalBackup"))?;
                unreachable!();
            }

            // If we found this TLI in the server's history before
            // encountering the latest TLI seen so far, this TLI is the
            // latest seen so far; symmetrically for the earliest.
            if !saw_latest_wal_range_tli {
                latest_wal_range_tli = range.tli;
            }
            if earliest_wal_range_tli == 0 || saw_earliest_wal_range_tli {
                earliest_wal_range_tli = range.tli;
                earliest_wal_range_start_lsn = range.start_lsn;
            }
        }

        // Propagate information about the prior backup into the backup_label
        // that will be generated for this backup (INCREMENTAL FROM LSN/TLI).
        backup_state.istartpoint = earliest_wal_range_start_lsn;
        backup_state.istarttli = earliest_wal_range_tli;

        // Sanity check start and end LSNs for the WAL ranges in the manifest
        // against this server's timeline switch points.
        for (i, range) in self.manifest_wal_ranges.iter().enumerate() {
            let tle = tlep[i].expect("matched above");

            if range.tli == earliest_wal_range_tli {
                if range.start_lsn < tle.begin {
                    ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "manifest requires WAL from initial timeline {} starting at {}, but that timeline begins at {}",
                            range.tli,
                            lsn_fmt(range.start_lsn),
                            lsn_fmt(tle.begin)
                        ))
                        .finish(loc("PrepareForIncrementalBackup"))?;
                    unreachable!();
                }
            } else if range.start_lsn != tle.begin {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "manifest requires WAL from continuation timeline {} starting at {}, but that timeline begins at {}",
                        range.tli,
                        lsn_fmt(range.start_lsn),
                        lsn_fmt(tle.begin)
                    ))
                    .finish(loc("PrepareForIncrementalBackup"))?;
                unreachable!();
            }

            if range.tli == latest_wal_range_tli {
                if range.end_lsn > backup_state.startpoint {
                    ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "manifest requires WAL from final timeline {} ending at {}, but this backup starts at {}",
                            range.tli,
                            lsn_fmt(range.end_lsn),
                            lsn_fmt(backup_state.startpoint)
                        ))
                        .errhint("This can happen for incremental backups on a standby if there was little activity since the previous backup.")
                        .finish(loc("PrepareForIncrementalBackup"))?;
                    unreachable!();
                }
            } else if range.end_lsn != tle.end {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "manifest requires WAL from non-final timeline {} ending at {}, but this server switched timelines at {}",
                        range.tli,
                        lsn_fmt(range.end_lsn),
                        lsn_fmt(tle.end)
                    ))
                    .finish(loc("PrepareForIncrementalBackup"))?;
                unreachable!();
            }
        }

        Ok((earliest_wal_range_tli, earliest_wal_range_start_lsn))
    }
}

/// The per-timeline summary-coverage loop of C's PrepareForIncrementalBackup:
/// walking the history from the current timeline backwards, find the
/// summaries needed on each timeline between the prior backup's start and
/// this backup's start, and error C-exactly when they are missing or
/// incomplete. Pure over its inputs; split out for unit testing.
#[allow(clippy::too_many_arguments)]
fn collect_required_summaries<'m>(
    mcx: Mcx<'m>,
    expected_tles: &[TimeLineHistoryEntry],
    all_wslist: &[WalSummaryFile],
    backup_start_tli: TimeLineID,
    backup_start_point: XLogRecPtr,
    earliest_wal_range_tli: TimeLineID,
    earliest_wal_range_start_lsn: XLogRecPtr,
) -> PgResult<PgVec<'m, WalSummaryFile>> {
    let mut required_wslist: PgVec<'m, WalSummaryFile> = PgVec::new_in(mcx);
    let mut found_backup_start_tli = false;

    for tle in expected_tles {
        let mut tli_start_lsn = tle.begin;
        let mut tli_end_lsn = tle.end;

        // Skip everything until we find the timeline where this backup
        // started (usually the first entry: latest timeline first).
        if tle.tli == backup_start_tli {
            found_backup_start_tli = true;
            tli_end_lsn = backup_start_point;
        } else if !found_backup_start_tli {
            continue;
        }

        // If this is the earliest timeline involved, the range of interest
        // begins with the start LSN of the prior backup.
        if tle.tli == earliest_wal_range_tli {
            tli_start_lsn = earliest_wal_range_start_lsn;
        }
        let tli_wslist = FilterWalSummaries(mcx, all_wslist, tle.tli, tli_start_lsn, tli_end_lsn);

        // There is no guarantee that the WAL summaries we found cover the
        // entire range of LSNs for which summaries are required.
        let (complete, tli_missing_lsn) =
            WalSummariesAreComplete(&tli_wslist, tli_start_lsn, tli_end_lsn);
        if !complete {
            if tli_missing_lsn == InvalidXLogRecPtr {
                ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "WAL summaries are required on timeline {} from {} to {}, but no summaries for that timeline and LSN range exist",
                        tle.tli,
                        lsn_fmt(tli_start_lsn),
                        lsn_fmt(tli_end_lsn)
                    ))
                    .finish(loc("PrepareForIncrementalBackup"))?;
                unreachable!();
            }
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "WAL summaries are required on timeline {} from {} to {}, but the summaries for that timeline and LSN range are incomplete",
                    tle.tli,
                    lsn_fmt(tli_start_lsn),
                    lsn_fmt(tli_end_lsn)
                ))
                .errdetail(format!(
                    "The first unsummarized LSN in this range is {}.",
                    lsn_fmt(tli_missing_lsn)
                ))
                .finish(loc("PrepareForIncrementalBackup"))?;
            unreachable!();
        }

        // Remember that we need to read these summaries. Redundant/
        // overlapping summaries only cost extra work, never correctness
        // (C keeps the same possibility open).
        for ws in tli_wslist.iter() {
            required_wslist.push(*ws);
        }

        // Timelines earlier than the one in which the prior backup began
        // are not relevant.
        if tle.tli == earliest_wal_range_tli {
            break;
        }
    }

    Ok(required_wslist)
}

/// The summary-merge tail of C's PrepareForIncrementalBackup: read every
/// required summary file (C: OpenWalSummaryFile + ReadWalSummary feeding
/// CreateBlockRefTableReader) and merge into one in-memory table.
/// `summaries_dir` is [`WAL_SUMMARIES_DIR`] in production; tests pass a
/// scratch directory.
fn merge_required_summaries<'mcx>(
    mcx: Mcx<'mcx>,
    required_wslist: &[WalSummaryFile],
    summaries_dir: &str,
) -> PgResult<BlockRefTable<'mcx>> {
    let mut brtab = BlockRefTable::new(mcx);

    for ws in required_wslist {
        // C: OpenWalSummaryFile(ws, false) — XLOGDIR/summaries/%08X%08X%08X%08X%08X.summary.
        let path = format!(
            "{summaries_dir}/{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
            ws.tli,
            (ws.start_lsn >> 32) as u32,
            ws.start_lsn as u32,
            (ws.end_lsn >> 32) as u32,
            ws.end_lsn as u32
        );
        let file = fd::PathNameOpenFile(&path, libc::O_RDONLY)?;
        if file.0 < 0 {
            ereport(ERROR)
                .with_saved_errno(fd::get_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m"))
                .finish(loc("PrepareForIncrementalBackup"))?;
            unreachable!();
        }
        // C: basebackup_incremental.c:584 — one DEBUG1 line per summary
        // file, naming the path it was opened under (FilePathName).
        ereport(DEBUG1)
            .errmsg_internal(format!("reading WAL summary file \"{path}\""))
            .finish(loc("PrepareForIncrementalBackup"))?;

        // C: ReadWalSummary — a positioned FileRead loop with C's error text.
        let mut filepos: i64 = 0;
        let read_cb = |buf: &mut [u8]| -> PgResult<usize> {
            let nbytes = fd::FileRead(file, buf, filepos, WAIT_EVENT_WAL_SUMMARY_READ)?;
            if nbytes < 0 {
                return ereport(ERROR)
                    .with_saved_errno(fd::get_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not read file \"{}\": %m", fd::FilePathName(file)))
                    .finish(loc("ReadWalSummary"))
                    .map(|()| 0);
            }
            filepos += nbytes as i64;
            Ok(nbytes as usize)
        };

        let mut reader = BlockRefTableReader::new(mcx, read_cb, &path)?;
        let mut blocks: [BlockNumber; BLOCKS_PER_READ] = [0; BLOCKS_PER_READ];
        while let Some((rlocator, forknum, limit_block)) = reader.next_relation()? {
            brtab.set_limit_block(rlocator, forknum, limit_block);

            loop {
                let nblocks = reader.get_blocks(&mut blocks)?;
                if nblocks == 0 {
                    break;
                }
                for &blkno in &blocks[..nblocks] {
                    brtab.mark_block_modified(rlocator, forknum, blkno)?;
                }
            }
        }
        drop(reader);
        fd::FileClose(file)?;
    }

    Ok(brtab)
}

// ===========================================================================
// Send-side consumers (C: the bottom half of basebackup_incremental.c):
// GetFileBackupMethod, GetIncrementalFilePath, GetIncrementalHeaderSize,
// GetIncrementalFileSize. Called from basebackup's sendDir/sendFile.
// ===========================================================================

/// C: INCREMENTAL_MAGIC (backup/basebackup_incremental.h). Native-endian on
/// the wire, like the rest of the incremental file header.
pub const INCREMENTAL_MAGIC: u32 = 0xd3ae1f0d;

/// C: RELSEG_SIZE (pg_config.h) — blocks per 1GB relation segment.
pub const RELSEG_SIZE: u32 = ((1024 * 1024 * 1024) / BLCKSZ) as u32;

/// C: enum FileBackupMethod (backup/basebackup_incremental.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileBackupMethod {
    /// C: BACK_UP_FILE_FULLY.
    Fully,
    /// C: BACK_UP_FILE_INCREMENTALLY.
    Incrementally,
}

/// C: GetIncrementalFilePath(dboid, spcoid, relfilenumber, forknum, segno) —
/// the pathname used when a file is sent incrementally.
///
/// C rebuilds the relation path from its locator via GetRelationPath and then
/// splices `INCREMENTAL.` in front of the last component (appending `.segno`
/// for segno > 0). The caller's `path` here is basebackup.c's `lookup_path`,
/// which is by construction the identical string GetRelationPath would
/// produce (including the `.segno` suffix carried by the directory entry
/// name), so splicing on `path` is value-identical to C.
pub fn GetIncrementalFilePath(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => format!("{}/INCREMENTAL.{}", &path[..i], &path[i + 1..]),
        // C: Assert(lastslash != NULL) — relation paths always contain '/'.
        None => format!("INCREMENTAL.{path}"),
    }
}

/// C: GetIncrementalHeaderSize — size of an incremental file header holding
/// `num_blocks_required` block numbers. Rounded up to a BLCKSZ multiple, but
/// only if the file will store some block data.
pub fn GetIncrementalHeaderSize(num_blocks_required: u32) -> usize {
    debug_assert!(num_blocks_required <= RELSEG_SIZE);

    // Three four-byte quantities (magic number, truncation block length,
    // block count) followed by block numbers.
    let mut result = 3 * 4 + 4 * num_blocks_required as usize;

    // Round the header size to a multiple of BLCKSZ - when not a multiple of
    // BLCKSZ, add the missing fraction of a block. But do this only if the
    // file will store data for some blocks, otherwise keep it small.
    if num_blocks_required > 0 && result % BLCKSZ != 0 {
        result += BLCKSZ - (result % BLCKSZ);
    }

    result
}

/// C: GetIncrementalFileSize — total size of an incremental file containing
/// a given number of blocks.
pub fn GetIncrementalFileSize(num_blocks_required: u32) -> usize {
    debug_assert!(num_blocks_required <= RELSEG_SIZE);
    GetIncrementalHeaderSize(num_blocks_required) + BLCKSZ * num_blocks_required as usize
}

impl IncrementalBackupInfo {
    /// C: GetFileBackupMethod(ib, path, dboid, spcoid, relfilenumber,
    /// forknum, segno, size, &num_blocks_required, relative_block_numbers,
    /// &truncation_block_length).
    ///
    /// How should we back up a particular file as part of an incremental
    /// backup?
    ///
    /// If the return value is [`FileBackupMethod::Fully`], caller should back
    /// up the whole file just as if this were not an incremental backup. The
    /// contents of the `relative_block_numbers` array are unspecified in this
    /// case.
    ///
    /// If the return value is [`FileBackupMethod::Incrementally`], caller
    /// should include an incremental file in the backup instead of the entire
    /// file. On return, `*num_blocks_required` will be set to the number of
    /// blocks that need to be sent, and the actual block numbers will have
    /// been stored in `relative_block_numbers`, which should be an array of
    /// at least RELSEG_SIZE. In addition, `*truncation_block_length` will be
    /// set to the value that should be included in the incremental file.
    ///
    /// `brtab` is the merged block-reference table returned by
    /// [`Self::PrepareForIncrementalBackup`] (C keeps it in `ib->brtab`; see
    /// the module comment for why it is threaded separately here).
    #[allow(clippy::too_many_arguments)]
    pub fn GetFileBackupMethod(
        &self,
        brtab: &BlockRefTable<'_>,
        path: &str,
        dboid: Oid,
        spcoid: Oid,
        relfilenumber: RelFileNumber,
        forknum: ForkNumber,
        segno: u32,
        size: u64,
        num_blocks_required: &mut u32,
        relative_block_numbers: &mut [BlockNumber],
        truncation_block_length: &mut u32,
    ) -> PgResult<FileBackupMethod> {
        // Should only be called after PrepareForIncrementalBackup.
        assert!(self.finalized, "GetFileBackupMethod before FinalizeIncrementalManifest");

        // dboid could be InvalidOid if shared rel, but spcoid and
        // relfilenumber should have legal values.
        debug_assert!(spcoid != 0);
        debug_assert!(relfilenumber != 0);

        // If the file size is too large or not a multiple of BLCKSZ, then
        // something weird is happening, so give up and send the whole file.
        if size % BLCKSZ as u64 != 0 || size / BLCKSZ as u64 > RELSEG_SIZE as u64 {
            return Ok(FileBackupMethod::Fully);
        }

        // The free-space map fork is not properly WAL-logged, so we need to
        // backup the entire file every time.
        if forknum == ForkNumber::FSM_FORKNUM {
            return Ok(FileBackupMethod::Fully);
        }

        // If this file was not part of the prior backup, back it up fully.
        //
        // If this file was created after the prior backup and before the
        // start of the current backup, then the WAL summary information will
        // tell us to back up the whole file. However, if this file was
        // created after the start of the current backup, then the WAL
        // summary won't know anything about it. Without this logic, we would
        // erroneously conclude that it was OK to send it incrementally.
        //
        // (The prior backup may itself have sent this file incrementally, in
        // which case the manifest carries the INCREMENTAL.* name.)
        if self.manifest_file_lookup(path.as_bytes()).is_none() {
            let ipath = GetIncrementalFilePath(path);
            if self.manifest_file_lookup(ipath.as_bytes()).is_none() {
                return Ok(FileBackupMethod::Fully);
            }
        }

        // Look up the special block reference table entry for the database
        // as a whole.
        let mut rlocator = RelFileLocator { spcOid: spcoid, dbOid: dboid, relNumber: 0 };
        if brtab.get_entry(rlocator, ForkNumber::MAIN_FORKNUM).is_some() {
            // According to the WAL summary, this database OID/tablespace OID
            // pairing has been created since the previous backup. So,
            // everything in it must be backed up fully.
            return Ok(FileBackupMethod::Fully);
        }

        // Look up the block reference table entry for this relfilenode.
        rlocator.relNumber = relfilenumber;
        let brtentry = brtab.get_entry(rlocator, forknum);

        // If there is no entry, then there have been no WAL-logged changes
        // to the relation since the predecessor backup was taken, so we can
        // back it up incrementally and need not include any modified blocks.
        //
        // However, if the file is zero-length, we should do a full backup,
        // because an incremental file is always more than zero length, and
        // it's silly to take an incremental backup when a full backup would
        // be smaller.
        let Some(brtentry) = brtentry else {
            if size == 0 {
                return Ok(FileBackupMethod::Fully);
            }
            *num_blocks_required = 0;
            *truncation_block_length = (size / BLCKSZ as u64) as u32;
            return Ok(FileBackupMethod::Incrementally);
        };
        let limit_block = brtentry.limit_block();

        // If the limit_block is less than or equal to the point where this
        // segment starts, send the whole file.
        if limit_block as u64 <= segno as u64 * RELSEG_SIZE as u64 {
            return Ok(FileBackupMethod::Fully);
        }

        // Get relevant entries from the block reference table entry.
        //
        // We shouldn't overflow computing the start or stop block numbers,
        // but if it manages to happen somehow, detect it and throw an error.
        // (C computes in unsigned 32-bit with defined wraparound; mirror.)
        let start_blkno = segno.wrapping_mul(RELSEG_SIZE);
        let stop_blkno = start_blkno.wrapping_add((size / BLCKSZ as u64) as u32);
        if start_blkno / RELSEG_SIZE != segno || stop_blkno < start_blkno {
            ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(format!(
                    "overflow computing block number bounds for segment {segno} with size {size}"
                ))
                .finish(loc("GetFileBackupMethod"))?;
            unreachable!();
        }

        // This will write *absolute* block numbers into the output array,
        // but we'll transpose them below.
        let nblocks =
            brtentry.get_blocks(start_blkno, stop_blkno, &mut relative_block_numbers[..RELSEG_SIZE as usize]);
        debug_assert!(nblocks <= RELSEG_SIZE as usize);

        // If we're going to have to send nearly all of the blocks, then just
        // send the whole file, because that won't require much extra storage
        // or transfer and will speed up and simplify backup restoration.
        // It's not clear what threshold is most appropriate here and perhaps
        // it ought to be configurable, but for now we're just going to say
        // that if we'd need to send 90% of the blocks anyway, give up and
        // send the whole file.
        //
        // NB: If you change the threshold here, at least make sure to back
        // up the file fully when every single block must be sent, because
        // there's nothing good about sending an incremental file in that
        // case. (C: `nblocks * BLCKSZ > size * 0.9`, a double comparison.)
        if (nblocks as u64 * BLCKSZ as u64) as f64 > size as f64 * 0.9 {
            return Ok(FileBackupMethod::Fully);
        }

        // Looks like we can send an incremental file, so sort the block
        // numbers and then transpose them from absolute block numbers to
        // relative block numbers if necessary.
        //
        // NB: If the block reference table was using the bitmap
        // representation for a given chunk, the block numbers in that chunk
        // will already be sorted, but when the array-of-offsets
        // representation is used, we can receive block numbers here out of
        // order.
        let out = &mut relative_block_numbers[..nblocks];
        out.sort_unstable();
        if start_blkno != 0 {
            for b in out.iter_mut() {
                *b -= start_blkno;
            }
        }
        *num_blocks_required = nblocks as u32;

        // The truncation block length is the minimum length of the
        // reconstructed file. Any block numbers below this threshold that
        // are not present in the backup need to be fetched from the prior
        // backup. At or above this threshold, blocks should only be included
        // in the result if they are present in the backup. (This may require
        // inserting zero blocks if the blocks included in the backup are
        // non-consecutive.)
        *truncation_block_length = (size / BLCKSZ as u64) as u32;
        if limit_block != InvalidBlockNumber {
            let relative_limit = limit_block - segno * RELSEG_SIZE;

            // We can't set a truncation_block_length in excess of the limit
            // block number (relativized to the current segment). To do so
            // would be to treat blocks from older backups as valid current
            // contents even if they were subsequently truncated away.
            if *truncation_block_length < relative_limit {
                *truncation_block_length = relative_limit;
            }

            // We also can't set a truncation_block_length in excess of the
            // segment size, since the reconstructed file can't be larger
            // than that.
            if *truncation_block_length > RELSEG_SIZE {
                *truncation_block_length = RELSEG_SIZE;
            }
        }

        // Send it incrementally.
        Ok(FileBackupMethod::Incrementally)
    }
}

/// The JsonManifestParseContext wired to an IncrementalBackupInfo (C: the
/// five manifest_* callbacks in basebackup_incremental.c).
struct IbManifestContext<'a> {
    ib: &'a mut IncrementalBackupInfo,
}

impl JsonManifestParseContext for IbManifestContext<'_> {
    /// C: manifest_process_version — incremental backups don't work with
    /// manifest version 1.
    fn version_cb(&mut self, manifest_version: i32) -> PgResult<()> {
        if manifest_version == 1 {
            return Err(elog::PgError::error(
                "backup manifest version 1 does not support incremental backup".to_string(),
            )
            .into());
        }
        Ok(())
    }

    /// C: manifest_process_system_identifier — validate against this server.
    fn system_identifier_cb(&mut self, manifest_system_identifier: u64) -> PgResult<()> {
        if manifest_system_identifier != self.ib.system_identifier {
            return Err(elog::PgError::error(format!(
                "system identifier in backup manifest is {}, but database system identifier is {}",
                manifest_system_identifier, self.ib.system_identifier
            ))
            .into());
        }
        Ok(())
    }

    /// C: manifest_process_file — store path and size, first entry wins;
    /// checksums and mtimes are dropped.
    fn per_file_cb(
        &mut self,
        pathname: &[u8],
        size: u64,
        _checksum_type: PgChecksumType,
        _checksum_payload: Option<&[u8]>,
    ) -> PgResult<()> {
        self.ib
            .manifest_files
            .entry(pathname.into())
            .or_insert(size);
        Ok(())
    }

    /// C: manifest_process_wal_range.
    fn per_wal_range_cb(
        &mut self,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) -> PgResult<()> {
        self.ib
            .manifest_wal_ranges
            .push(BackupWalRange { tli, start_lsn, end_lsn });
        Ok(())
    }

    // error_cb: the default (plain ERROR with the C text) matches C's
    // manifest_report_error, which wraps every message in errmsg_internal.
}

#[cfg(test)]
mod tests;
