//! C: src/backend/backup/basebackup_incremental.c (PG 18.3) — support code
//! for incremental base backups: the `IncrementalBackupInfo` built from the
//! manifest a client supplies via UPLOAD_MANIFEST, and
//! `PrepareForIncrementalBackup`, which cross-checks the summarized-WAL
//! coverage and determines the LSN from which the incremental is taken.
//!
//! Stage-3 scope (incremental-basebackup-port.md): CreateIncrementalBackupInfo,
//! AppendIncrementalManifestData, FinalizeIncrementalManifest,
//! PrepareForIncrementalBackup. The send-side consumers (GetFileBackupMethod,
//! GetIncrementalFilePath, GetIncrementalHeaderSize/GetIncrementalFileSize)
//! are Stage 4, which also wires the basebackup sendFile path.
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
//!   accumulates verbatim (Q3 ruling: no cap, no spill — C-compatible
//!   unbounded acceptance) and `FinalizeIncrementalManifest` runs one
//!   whole-buffer parse. The checksum covers the same bytes and every error
//!   string is unchanged, so the difference is not client-visible.
//! - **brtab lifetime.** C stashes the merged block-reference table in
//!   `ib->brtab` for GetFileBackupMethod. Here `PrepareForIncrementalBackup`
//!   *returns* the `BlockRefTable`, tied to the caller's (command-lifetime)
//!   memory context — Stage 4 threads it to GetFileBackupMethod alongside
//!   `&IncrementalBackupInfo`. This avoids a self-referential
//!   session-object/arena pairing (the C1 decode-arena lifetime trap).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::collections::HashMap;

use blkreftable::{BlockRefTable, BlockRefTableReader};
use elog::ereport;
use manifest::checksum::PgChecksumType;
use mcx::{Mcx, MemoryContext, PgVec};
use parse_manifest::{json_parse_manifest, JsonManifestParseContext};
use timeline_seams::TimeLineHistoryEntry;
use types_core::{BlockNumber, TimeLineID, XLogRecPtr};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
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
    /// Grows unbounded, verbatim C behavior (Q3 ruling).
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

impl IncrementalBackupInfo {
    /// C: AppendIncrementalManifestData — each chunk of manifest data
    /// received from the client is passed here. C interleaves incremental
    /// parsing (MIN_CHUNK/MAX_CHUNK); we accumulate and parse in
    /// FinalizeIncrementalManifest (see module comment).
    pub fn AppendIncrementalManifestData(&mut self, data: &[u8]) {
        debug_assert!(!self.finalized, "append after FinalizeIncrementalManifest");
        self.buf.extend_from_slice(data);
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

    /// The number of file entries taken from the manifest (Stage-4's
    /// GetFileBackupMethod consumes the lookups; tests consume the count).
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
