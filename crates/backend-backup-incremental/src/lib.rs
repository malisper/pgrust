//! Port of `src/backend/backup/basebackup_incremental.c` (PostgreSQL 18.3).
//!
//! This code isn't in charge of taking an incremental backup; the actual
//! construction happens in `basebackup.c`. Here we provide the supports for
//! that operation: ingest the backup manifest supplied by the client taking the
//! incremental backup and extract the required information, decide which WAL
//! summaries are needed and merge them into a single in-memory
//! block-reference table, and answer the per-file "back up fully vs.
//! incrementally" question (and, if incrementally, which blocks).
//!
//! The in-memory block-reference table (`common/blkreftable.c`), the streaming
//! manifest JSON parser (`common/parse_manifest.c`), the WAL-summary file
//! listing / filtering / reading (`backup/walsummary.c`), and the
//! summarization-wait / timeline-history / system-identifier subsystems are all
//! external owners reached through their seam crates. All of the per-file /
//! per-block decision logic and the manifest sanity checks are owned here.

#![allow(non_snake_case)]

use backend_common_relpath::GetRelationPath;
use backend_utils_error::ereport;
use common_hashfn::hash_bytes;
use mcx::Mcx;
use std::collections::HashMap;
use types_blkreftable::BlockRefTable;
use types_core::{
    BlockNumber, ForkNumber, InvalidBlockNumber, InvalidOid, Oid, RelFileNumber, TimeLineID,
    XLogRecPtr, BLCKSZ, FSM_FORKNUM, INVALID_PROC_NUMBER, MAIN_FORKNUM,
};
use types_error::{
    ErrorLocation, PgResult, DEBUG1, ERRCODE_INTERNAL_ERROR,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use types_parse_manifest::{
    JsonManifestParseIncrementalStateHandle, ManifestFileRecord, ParsedManifestChunk,
};
use types_storage::smgr::RELSEG_SIZE;
use types_storage::RelFileLocator;
use types_wal::BackupState;

use common_blkreftable_seams as blkreftable;
use common_parse_manifest_seams as parse_manifest;
use backend_backup_walsummary_seams as walsummary;
use backend_postmaster_walsummarizer_seams as walsummarizer;
use backend_access_transam_timeline_seams as timeline;
use backend_access_transam_xlog_seams as xlog;

/// `INCREMENTAL_MAGIC` (basebackup_incremental.h:20).
pub const INCREMENTAL_MAGIC: u32 = 0xd3ae_1f0d;

/// `InvalidXLogRecPtr` (xlogdefs.h).
const InvalidXLogRecPtr: XLogRecPtr = 0;

/// `#define BLOCKS_PER_READ 512`
const BLOCKS_PER_READ: usize = 512;

/// We expect to find the last lines of the manifest, including the checksum, in
/// the last `MIN_CHUNK` bytes of the manifest. We trigger an incremental parse
/// step if we are about to overflow `MAX_CHUNK` bytes.
const MIN_CHUNK: usize = 1024;
const MAX_CHUNK: usize = 128 * 1024;

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

#[inline]
fn RelFileNumberIsValid(relnumber: RelFileNumber) -> bool {
    relnumber != InvalidOid
}

/// `BlockNumberIsValid(blockNumber)` (block.h).
#[inline]
fn BlockNumberIsValid(block_number: BlockNumber) -> bool {
    block_number != InvalidBlockNumber
}

/// `XLogRecPtrIsInvalid(r)` (xlogdefs.h).
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `LSN_FORMAT_ARGS(lsn)` rendered as the `"%X/%X"` text.
#[inline]
fn lsn(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

fn here(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("basebackup_incremental.rs", 0, func)
}

/// `FileBackupMethod` (basebackup_incremental.h:22-26).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileBackupMethod {
    /// `BACK_UP_FILE_FULLY`
    BackUpFileFully,
    /// `BACK_UP_FILE_INCREMENTALLY`
    BackUpFileIncrementally,
}

/// Details extracted from the WAL ranges present in the supplied backup manifest
/// (file-static `backup_wal_range`).
#[derive(Clone, Copy, Debug)]
struct BackupWalRange {
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
}

/// Details extracted from the file list present in the supplied backup manifest
/// (file-static `backup_file_entry`). C stores `path` and `size` in the
/// simplehash but only ever looks up by path afterward (the size is retained
/// "for sanity checking" per the struct comment, never re-read in this file).
/// We keep both fields for faithful parity; `size` is deliberately
/// retained-but-unread, exactly as in C.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct BackupFileEntry {
    path: String,
    size: u64,
}

/// `struct IncrementalBackupInfo`.
///
/// The C `MemoryContext mcxt` is threaded as the [`Mcx`] used for the
/// owner-seam allocations (block-reference table, reader blocks, parser
/// records). The manifest staging buffer (`StringInfoData buf`), WAL ranges
/// (`List *manifest_wal_ranges`), manifest file hash (`backup_file_hash
/// *manifest_files`), and block-reference table (`BlockRefTable *brtab`, the
/// genuine owned value) are owned here.
pub struct IncrementalBackupInfo<'mcx> {
    /// Memory context for this object and its subsidiary objects.
    mcx: Mcx<'mcx>,

    /// Temporary buffer for storing the manifest while parsing it
    /// (`StringInfoData buf`). Set to `None` after
    /// [`Self::finalize_incremental_manifest`] to mirror `ib->buf.data == NULL`,
    /// which downstream `Assert`s check.
    buf: Option<Vec<u8>>,

    /// State object for incremental JSON parsing
    /// (`JsonManifestParseIncrementalState *inc_state`).
    inc_state: JsonManifestParseIncrementalStateHandle,

    /// Records decoded by the streaming JSON parser so far, accumulated across
    /// every incremental parse step. Replayed through the `manifest_process_*`
    /// callbacks at finalize time, in document order, exactly as C invokes the
    /// callbacks from inside `json_parse_manifest_incremental_chunk`.
    parsed: ParsedManifestChunk,

    /// WAL ranges extracted from the backup manifest (`List
    /// *manifest_wal_ranges`).
    manifest_wal_ranges: Vec<BackupWalRange>,

    /// Files extracted from the backup manifest (`backup_file_hash
    /// *manifest_files`). Keyed by path; we retain only path + size.
    manifest_files: HashMap<String, BackupFileEntry>,

    /// Block-reference table for the incremental backup (`BlockRefTable
    /// *brtab`), populated by [`Self::prepare_for_incremental_backup`]. The
    /// genuine owned table (the C `BlockRefTable *`), held for the lifetime of
    /// this `IncrementalBackupInfo`.
    brtab: Option<BlockRefTable>,
}

/// `hash_string_pointer` — helper for the filemap hash table. Retained even
/// though this port keys the manifest map on `String` directly, because C uses
/// this exact hash for the simplehash. (basebackup_incremental.c:930-936)
#[allow(dead_code)]
fn hash_string_pointer(s: &str) -> u32 {
    hash_bytes(s.as_bytes())
}

impl<'mcx> IncrementalBackupInfo<'mcx> {
    /// `CreateIncrementalBackupInfo(MemoryContext mcxt)`.
    ///
    /// Allocates the object, initializes the manifest staging buffer and the
    /// manifest-file map, wires the `JsonManifestParseContext` callbacks (the
    /// `manifest_process_*` functions, owned here and replayed at finalize
    /// time), and calls `json_parse_manifest_incremental_init`.
    /// (basebackup_incremental.c:151-186)
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        IncrementalBackupInfo {
            mcx,
            buf: Some(Vec::new()),
            inc_state: parse_manifest::json_parse_manifest_incremental_init::call(),
            parsed: ParsedManifestChunk::default(),
            manifest_wal_ranges: Vec::new(),
            manifest_files: HashMap::new(),
            brtab: None,
        }
    }

    /// `AppendIncrementalManifestData(ib, data, len)`.
    ///
    /// Each chunk of manifest data received from the client is passed here.
    /// When the buffer is about to overflow `MAX_CHUNK`, C performs an
    /// incremental JSON parse step over all but the last `MIN_CHUNK` bytes (so
    /// enough remains for the final piece), then `memmove`s the retained tail
    /// to the front and resets the length to `MIN_CHUNK`. We preserve that
    /// behavior, feeding the all-but-`MIN_CHUNK` prefix to the streaming-parser
    /// seam and merging the decoded records into [`Self::parsed`].
    /// (basebackup_incremental.c:193-220)
    pub fn append_incremental_manifest_data(&mut self, data: &[u8]) -> PgResult<()> {
        let buf = self
            .buf
            .as_mut()
            .expect("AppendIncrementalManifestData after FinalizeIncrementalManifest");

        if buf.len() > MIN_CHUNK && buf.len() + data.len() > MAX_CHUNK {
            // Time for an incremental parse. We'll do all but the last MIN_CHUNK
            // so that we have enough left for the final piece.
            let split = buf.len() - MIN_CHUNK;
            let decoded = parse_manifest::json_parse_manifest_incremental_chunk::call(
                self.inc_state,
                &buf[..split],
                false,
            )?;
            merge_parsed(&mut self.parsed, decoded);

            // Now remove what we just parsed: memmove the retained tail (the
            // last MIN_CHUNK bytes) to the front and set buf.len = MIN_CHUNK. C
            // copies MIN_CHUNK + 1 bytes (including the StringInfo trailing NUL);
            // the owned Vec has no NUL terminator, so we retain exactly
            // MIN_CHUNK.
            buf.drain(..split);
        }

        buf.extend_from_slice(data);
        Ok(())
    }

    /// `FinalizeIncrementalManifest(ib)`.
    ///
    /// Parse the last chunk of the manifest (`is_last = true`), then shut the
    /// incremental parser down; release the staging buffer (setting it to
    /// `None` so the `ib->buf.data == NULL` assertions downstream hold). The
    /// replay of the decoded records through the `manifest_process_*` callbacks
    /// happens here, in document order, exactly as C invokes the callbacks from
    /// inside the parser. (basebackup_incremental.c:226-247)
    pub fn finalize_incremental_manifest(&mut self) -> PgResult<()> {
        let buf = self
            .buf
            .take()
            .expect("FinalizeIncrementalManifest called twice");

        // Parse the last chunk (json_parse_manifest_incremental_chunk with
        // is_last = true).
        let decoded = parse_manifest::json_parse_manifest_incremental_chunk::call(
            self.inc_state,
            &buf,
            true,
        )?;
        merge_parsed(&mut self.parsed, decoded);

        // Done with the buffer, so release memory (ib->buf.data = NULL).
        // Already taken above.

        // Done with inc_state, so release that memory too.
        parse_manifest::json_parse_manifest_incremental_shutdown::call(self.inc_state);

        let parsed = core::mem::take(&mut self.parsed);
        self.replay_manifest(parsed)
    }

    /// Replay the manifest data callbacks (`manifest_process_*`) over the
    /// records decoded by the external JSON parser, in document order. This is
    /// the in-crate ownership of those callbacks; the seam only tokenizes.
    fn replay_manifest(&mut self, parsed: ParsedManifestChunk) -> PgResult<()> {
        if let Some(version) = parsed.version {
            self.manifest_process_version(version)?;
        }
        if let Some(system_identifier) = parsed.system_identifier {
            self.manifest_process_system_identifier(system_identifier)?;
        }
        for file in &parsed.files {
            self.manifest_process_file(file);
        }
        for range in &parsed.wal_ranges {
            self.manifest_process_wal_range(range.tli, range.start_lsn, range.end_lsn);
        }
        Ok(())
    }

    /// `PrepareForIncrementalBackup(ib, backup_state)`.
    ///
    /// Performs sanity checks on the data extracted from the manifest, figures
    /// out for which WAL ranges we need summaries and whether they're available,
    /// reads and combines the data from those summary files, and updates
    /// `backup_state` with the reference TLI and LSN for the prior backup.
    /// (basebackup_incremental.c:262-617)
    pub fn prepare_for_incremental_backup(
        &mut self,
        backup_state: &mut BackupState,
    ) -> PgResult<()> {
        let mut found_backup_start_tli = false;
        let mut earliest_wal_range_tli: TimeLineID = 0;
        let mut earliest_wal_range_start_lsn: XLogRecPtr = InvalidXLogRecPtr;
        let mut latest_wal_range_tli: TimeLineID = 0;

        assert!(self.buf.is_none());

        // A valid backup manifest must always contain at least one WAL range
        // (usually exactly one, unless the backup spanned a timeline switch).
        let num_wal_ranges = self.manifest_wal_ranges.len();
        if num_wal_ranges == 0 {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("manifest contains no required WAL ranges")
                .finish(here("PrepareForIncrementalBackup"));
        }

        // Match up the TLIs that appear in the WAL ranges of the backup
        // manifest with those that appear in this server's timeline history. We
        // expect every backup_wal_range to match to a TimeLineHistoryEntry; if
        // it does not, that's an error.
        //
        // Note that the return value of readTimeLineHistory puts the latest
        // timeline at the beginning of the list, not the end. Hence, the
        // earliest TLI is the one that occurs nearest the end of the list, and
        // the latest TLI is the one closest to the beginning.
        let expectedTLEs = timeline::read_timeline_history::call(self.mcx, backup_state.starttli())?;
        // tlep[i] = index into expectedTLEs, or None (C uses TimeLineHistoryEntry *).
        let mut tlep: Vec<Option<usize>> = vec![None; num_wal_ranges];
        for i in 0..num_wal_ranges {
            let range = self.manifest_wal_ranges[i];
            let mut saw_earliest_wal_range_tli = false;
            let mut saw_latest_wal_range_tli = false;

            // Search this server's history for this WAL range's TLI.
            for (idx, tle) in expectedTLEs.iter().enumerate() {
                if tle.tli == range.tli {
                    tlep[i] = Some(idx);
                    break;
                }

                if tle.tli == earliest_wal_range_tli {
                    saw_earliest_wal_range_tli = true;
                }
                if tle.tli == latest_wal_range_tli {
                    saw_latest_wal_range_tli = true;
                }
            }

            // An incremental backup can only be taken relative to a backup that
            // represents a previous state of this server. If the backup requires
            // WAL from a timeline that's not in our history, that definitely
            // isn't the case.
            if tlep[i].is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "timeline {} found in manifest, but not in this server's history",
                        range.tli
                    ))
                    .finish(here("PrepareForIncrementalBackup"));
            }

            // If we found this TLI in the server's history before encountering
            // the latest TLI seen so far, then this TLI is the latest one seen
            // so far. If we saw the earliest TLI seen so far before finding this
            // TLI, this TLI is earlier than the earliest one seen so far. And if
            // this is the first TLI for which we've searched, it's also the
            // earliest one seen so far.
            if !saw_latest_wal_range_tli {
                latest_wal_range_tli = range.tli;
            }
            if earliest_wal_range_tli == 0 || saw_earliest_wal_range_tli {
                earliest_wal_range_tli = range.tli;
                earliest_wal_range_start_lsn = range.start_lsn;
            }
        }

        // Propagate information about the prior backup into the backup_label
        // that will be generated for this backup.
        backup_state.set_istartpoint(earliest_wal_range_start_lsn);
        backup_state.set_istarttli(earliest_wal_range_tli);

        // Sanity check start and end LSNs for the WAL ranges in the manifest.
        for i in 0..num_wal_ranges {
            let range = self.manifest_wal_ranges[i];
            let tle = expectedTLEs[tlep[i].expect("tlep[i] checked non-null above")];

            if range.tli == earliest_wal_range_tli {
                if range.start_lsn < tle.begin {
                    return ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "manifest requires WAL from initial timeline {} starting at {}, but that timeline begins at {}",
                            range.tli,
                            lsn(range.start_lsn),
                            lsn(tle.begin)
                        ))
                        .finish(here("PrepareForIncrementalBackup"));
                }
            } else if range.start_lsn != tle.begin {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "manifest requires WAL from continuation timeline {} starting at {}, but that timeline begins at {}",
                        range.tli,
                        lsn(range.start_lsn),
                        lsn(tle.begin)
                    ))
                    .finish(here("PrepareForIncrementalBackup"));
            }

            if range.tli == latest_wal_range_tli {
                if range.end_lsn > backup_state.startpoint() {
                    return ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "manifest requires WAL from final timeline {} ending at {}, but this backup starts at {}",
                            range.tli,
                            lsn(range.end_lsn),
                            lsn(backup_state.startpoint())
                        ))
                        .errhint("This can happen for incremental backups on a standby if there was little activity since the previous backup.")
                        .finish(here("PrepareForIncrementalBackup"));
                }
            } else if range.end_lsn != tle.end {
                return ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "manifest requires WAL from non-final timeline {} ending at {}, but this server switched timelines at {}",
                        range.tli,
                        lsn(range.end_lsn),
                        lsn(tle.end)
                    ))
                    .finish(here("PrepareForIncrementalBackup"));
            }
        }

        // Wait for WAL summarization to catch up to the backup start LSN. This
        // will throw an error if the WAL summarizer appears to be stuck. If WAL
        // summarization gets disabled while we're waiting, this will return
        // immediately, and we'll error out further down if the WAL summaries are
        // incomplete.
        walsummarizer::wait_for_wal_summarization::call(backup_state.startpoint())?;

        // Retrieve a list of all WAL summaries on any timeline that overlap with
        // the LSN range of interest. We could instead call GetWalSummaries()
        // once per timeline in the loop that follows, but that would involve
        // reading the directory multiple times.
        let all_wslist = walsummary::get_wal_summaries::call(
            self.mcx,
            0,
            earliest_wal_range_start_lsn,
            backup_state.startpoint(),
        )?;

        // We need WAL summaries for everything that happened during the prior
        // backup and everything that happened afterward up until the point where
        // the current backup started.
        let mut required_wslist: Vec<types_walsummarizer::WalSummaryFile> = Vec::new();
        for tle in expectedTLEs.iter() {
            let mut tli_start_lsn: XLogRecPtr = tle.begin;
            let mut tli_end_lsn: XLogRecPtr = tle.end;

            // Working through the history of this server from the current
            // timeline backwards, we skip everything until we find the timeline
            // where this backup started. Most of the time, this means we won't
            // skip anything at all.
            if tle.tli == backup_state.starttli() {
                found_backup_start_tli = true;
                tli_end_lsn = backup_state.startpoint();
            } else if !found_backup_start_tli {
                continue;
            }

            // Find the summaries that overlap the LSN range of interest for this
            // timeline. If this is the earliest timeline involved, the range of
            // interest begins with the start LSN of the prior backup; otherwise,
            // it begins at the LSN at which this timeline came into existence.
            if tle.tli == earliest_wal_range_tli {
                tli_start_lsn = earliest_wal_range_start_lsn;
            }
            let tli_wslist = backend_backup_walsummary::filter_wal_summaries(
                self.mcx,
                &all_wslist,
                tle.tli,
                tli_start_lsn,
                tli_end_lsn,
            )?;

            // There is no guarantee that the WAL summaries we found cover the
            // entire range of LSNs for which summaries are required, or indeed
            // that we found any WAL summaries at all. Check whether we have a
            // problem of that sort.
            let mut tli_missing_lsn: XLogRecPtr = InvalidXLogRecPtr;
            let complete = backend_backup_walsummary::wal_summaries_are_complete(
                &tli_wslist,
                tli_start_lsn,
                tli_end_lsn,
                &mut tli_missing_lsn,
            );
            if !complete {
                if XLogRecPtrIsInvalid(tli_missing_lsn) {
                    return ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "WAL summaries are required on timeline {} from {} to {}, but no summaries for that timeline and LSN range exist",
                            tle.tli,
                            lsn(tli_start_lsn),
                            lsn(tli_end_lsn)
                        ))
                        .finish(here("PrepareForIncrementalBackup"));
                } else {
                    return ereport(ERROR)
                        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                        .errmsg(format!(
                            "WAL summaries are required on timeline {} from {} to {}, but the summaries for that timeline and LSN range are incomplete",
                            tle.tli,
                            lsn(tli_start_lsn),
                            lsn(tli_end_lsn)
                        ))
                        .errdetail(format!(
                            "The first unsummarized LSN in this range is {}.",
                            lsn(tli_missing_lsn)
                        ))
                        .finish(here("PrepareForIncrementalBackup"));
                }
            }

            // Remember that we need to read these summaries.
            required_wslist.extend(tli_wslist.iter().copied());

            // Timelines earlier than the one in which the prior backup began are
            // not relevant.
            if tle.tli == earliest_wal_range_tli {
                break;
            }
        }

        // Read all of the required block reference table files and merge all of
        // the data into a single in-memory block reference table.
        let mut brtab = blkreftable::create_empty_block_ref_table::call(self.mcx)?;
        for ws in &required_wslist {
            // OpenWalSummaryFile(ws, false) + CreateBlockRefTableReader(
            // ReadWalSummary, &io, FilePathName(io.file), ReportWalSummaryError,
            // NULL): bundled into the walsummary owner seam, which returns the
            // owned reader plus the open File (the reader's read callback
            // captured a copy of the File; the File is threaded back to the
            // FileClose teardown).
            let (mut reader, file) = walsummary::wal_summary_create_reader::call(self.mcx, *ws)?;
            // C logs `FilePathName(wsio.file)` (the open File's path); the File
            // lives below the seam, so this DEBUG1 message identifies the summary
            // by its (tli, start_lsn, end_lsn) instead. Log text only.
            ereport(DEBUG1)
                .errmsg_internal(format!(
                    "reading WAL summary file for timeline {} from {} to {}",
                    ws.tli,
                    lsn(ws.start_lsn),
                    lsn(ws.end_lsn)
                ))
                .finish(here("PrepareForIncrementalBackup"))?;

            while let Some((rlocator, forknum, limit_block)) =
                blkreftable::block_ref_table_reader_next_relation::call(&mut reader)?
            {
                blkreftable::block_ref_table_set_limit_block::call(
                    &mut brtab, rlocator, forknum, limit_block,
                )?;

                loop {
                    let blocks = blkreftable::block_ref_table_reader_get_blocks::call(
                        self.mcx,
                        &mut reader,
                        BLOCKS_PER_READ,
                    )?;
                    if blocks.is_empty() {
                        break;
                    }
                    for &blkno in blocks.iter() {
                        blkreftable::block_ref_table_mark_block_modified::call(
                            &mut brtab, rlocator, forknum, blkno,
                        )?;
                    }
                }
            }

            // DestroyBlockRefTableReader(reader) + FileClose(wsio.file). The
            // reader is consumed (dropped, freeing its buffers + read callback);
            // the File is threaded back to its FileClose teardown.
            blkreftable::destroy_block_ref_table_reader::call(reader);
            walsummary::wal_summary_reader_file_close::call(file);
        }
        self.brtab = Some(brtab);

        Ok(())
    }

    /// `GetFileBackupMethod(ib, path, dboid, spcoid, relfilenumber, forknum,
    /// segno, size, *num_blocks_required, relative_block_numbers,
    /// *truncation_block_length)`.
    ///
    /// `relative_block_numbers` must be a slice of at least `RELSEG_SIZE`
    /// elements. On [`FileBackupMethod::BackUpFileIncrementally`],
    /// `num_blocks_required` and `truncation_block_length` are set and the block
    /// numbers are written into `relative_block_numbers[0..*num_blocks_required]`.
    /// (basebackup_incremental.c:662-873)
    #[allow(clippy::too_many_arguments)]
    pub fn get_file_backup_method(
        &self,
        path: &str,
        dboid: Oid,
        spcoid: Oid,
        relfilenumber: RelFileNumber,
        forknum: ForkNumber,
        segno: u32,
        size: usize,
        num_blocks_required: &mut u32,
        relative_block_numbers: &mut [BlockNumber],
        truncation_block_length: &mut u32,
    ) -> PgResult<FileBackupMethod> {
        // Should only be called after PrepareForIncrementalBackup.
        assert!(self.buf.is_none());

        // dboid could be InvalidOid if shared rel, but spcoid and relfilenumber
        // should have legal values.
        assert!(OidIsValid(spcoid));
        assert!(RelFileNumberIsValid(relfilenumber));

        let brtab = self
            .brtab
            .as_ref()
            .expect("GetFileBackupMethod before PrepareForIncrementalBackup");

        // If the file size is too large or not a multiple of BLCKSZ, then
        // something weird is happening, so give up and send the whole file.
        if (size % BLCKSZ) != 0 || (size / BLCKSZ) as u64 > RELSEG_SIZE as u64 {
            return Ok(FileBackupMethod::BackUpFileFully);
        }

        // The free-space map fork is not properly WAL-logged, so we need to
        // backup the entire file every time.
        if forknum == FSM_FORKNUM {
            return Ok(FileBackupMethod::BackUpFileFully);
        }

        // If this file was not part of the prior backup, back it up fully.
        //
        // If this file was created after the prior backup and before the start
        // of the current backup, then the WAL summary information will tell us
        // to back up the whole file. However, if this file was created after the
        // start of the current backup, then the WAL summary won't know anything
        // about it. Without this logic, we would erroneously conclude that it
        // was OK to send it incrementally.
        if self.backup_file_lookup(path).is_none() {
            let ipath = GetIncrementalFilePath(dboid, spcoid, relfilenumber, forknum, segno);
            if self.backup_file_lookup(&ipath).is_none() {
                return Ok(FileBackupMethod::BackUpFileFully);
            }
        }

        // Look up the special block reference table entry for the database as a
        // whole.
        let rlocator = RelFileLocator::new(spcoid, dboid, 0);
        if blkreftable::block_ref_table_get_entry::call(brtab, rlocator, MAIN_FORKNUM).is_some() {
            // According to the WAL summary, this database OID/tablespace OID
            // pairing has been created since the previous backup. So, everything
            // in it must be backed up fully.
            return Ok(FileBackupMethod::BackUpFileFully);
        }

        // Look up the block reference table entry for this relfilenode.
        let rlocator = RelFileLocator::new(spcoid, dboid, relfilenumber);
        let limit_block =
            match blkreftable::block_ref_table_get_entry::call(brtab, rlocator, forknum) {
                // If there is no entry, then there have been no WAL-logged
                // changes to the relation since the predecessor backup was
                // taken, so we can back it up incrementally and need not include
                // any modified blocks.
                //
                // However, if the file is zero-length, we should do a full
                // backup, because an incremental file is always more than zero
                // length, and it's silly to take an incremental backup when a
                // full backup would be smaller.
                None => {
                    if size == 0 {
                        return Ok(FileBackupMethod::BackUpFileFully);
                    }
                    *num_blocks_required = 0;
                    *truncation_block_length = (size / BLCKSZ) as u32;
                    return Ok(FileBackupMethod::BackUpFileIncrementally);
                }
                Some(limit_block) => limit_block,
            };

        // If the limit_block is less than or equal to the point where this
        // segment starts, send the whole file.
        if limit_block <= segno.wrapping_mul(RELSEG_SIZE) {
            return Ok(FileBackupMethod::BackUpFileFully);
        }

        // Get relevant entries from the block reference table entry.
        //
        // We shouldn't overflow computing the start or stop block numbers, but
        // if it manages to happen somehow, detect it and throw an error.
        let start_blkno: BlockNumber = segno.wrapping_mul(RELSEG_SIZE);
        let stop_blkno: BlockNumber = start_blkno.wrapping_add((size / BLCKSZ) as u32);
        if start_blkno / RELSEG_SIZE != segno || stop_blkno < start_blkno {
            return ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!(
                    "overflow computing block number bounds for segment {} with size {}",
                    segno, size
                ))
                .finish(here("GetFileBackupMethod"))
                .map(|()| unreachable!("ERROR always returns Err"));
        }

        // This will write *absolute* block numbers into the output array, but
        // we'll transpose them below. The combined GetEntry+GetBlocks seam
        // re-looks-up the entry (confirmed to exist above) inside the owner and
        // returns its blocks in `[start_blkno, stop_blkno)`.
        let blocks = blkreftable::block_ref_table_get_entry_blocks::call(
            self.mcx,
            brtab,
            rlocator,
            forknum,
            start_blkno,
            stop_blkno,
            RELSEG_SIZE as usize,
        )?
        .map(|(_limit, blocks)| blocks)
        .expect("entry confirmed present by the preceding BlockRefTableGetEntry");
        let nblocks = blocks.len();
        assert!(nblocks <= RELSEG_SIZE as usize);
        relative_block_numbers[..nblocks].copy_from_slice(&blocks);

        // If we're going to have to send nearly all of the blocks, then just
        // send the whole file, because that won't require much extra storage or
        // transfer and will speed up and simplify backup restoration. For now we
        // say that if we'd need to send 90% of the blocks anyway, give up and
        // send the whole file.
        if (nblocks * BLCKSZ) as f64 > size as f64 * 0.9 {
            return Ok(FileBackupMethod::BackUpFileFully);
        }

        // Looks like we can send an incremental file, so sort the block numbers
        // and then transpose them from absolute block numbers to relative block
        // numbers if necessary.
        //
        // NB: If the block reference table was using the bitmap representation
        // for a given chunk, the block numbers in that chunk will already be
        // sorted, but when the array-of-offsets representation is used, we can
        // receive block numbers here out of order.
        relative_block_numbers[..nblocks].sort_by(|a, b| compare_block_numbers(*a, *b).cmp(&0));
        if start_blkno != 0 {
            for slot in relative_block_numbers[..nblocks].iter_mut() {
                *slot -= start_blkno;
            }
        }
        *num_blocks_required = nblocks as u32;

        // The truncation block length is the minimum length of the
        // reconstructed file. Any block numbers below this threshold that are
        // not present in the backup need to be fetched from the prior backup. At
        // or above this threshold, blocks should only be included in the result
        // if they are present in the backup.
        *truncation_block_length = (size / BLCKSZ) as u32;
        if BlockNumberIsValid(limit_block) {
            let relative_limit: u32 = limit_block - segno.wrapping_mul(RELSEG_SIZE);

            // We can't set a truncation_block_length in excess of the limit
            // block number (relativized to the current segment). To do so would
            // be to treat blocks from older backups as valid current contents
            // even if they were subsequently truncated away.
            if *truncation_block_length < relative_limit {
                *truncation_block_length = relative_limit;
            }

            // We also can't set a truncation_block_length in excess of the
            // segment size, since the reconstructed file can't be larger than
            // that.
            if *truncation_block_length > RELSEG_SIZE {
                *truncation_block_length = RELSEG_SIZE;
            }
        }

        // Send it incrementally.
        Ok(FileBackupMethod::BackUpFileIncrementally)
    }

    /// `backup_file_lookup(ib->manifest_files, path)` — simplehash lookup.
    fn backup_file_lookup(&self, path: &str) -> Option<&BackupFileEntry> {
        self.manifest_files.get(path)
    }

    /// `manifest_process_version` — validate the manifest version for
    /// incremental backup. (basebackup_incremental.c:941-949)
    fn manifest_process_version(&self, manifest_version: i32) -> PgResult<()> {
        // Incremental backups don't work with manifest version 1.
        if manifest_version == 1 {
            return manifest_report_error(
                "backup manifest version 1 does not support incremental backup",
            );
        }
        Ok(())
    }

    /// `manifest_process_system_identifier` — validate the manifest system
    /// identifier against the current database server.
    /// (basebackup_incremental.c:955-969)
    fn manifest_process_system_identifier(&self, manifest_system_identifier: u64) -> PgResult<()> {
        // Get system identifier of current system.
        let system_identifier = xlog::get_system_identifier::call();

        if manifest_system_identifier != system_identifier {
            return manifest_report_error(format!(
                "system identifier in backup manifest is {}, but database system identifier is {}",
                manifest_system_identifier, system_identifier
            ));
        }
        Ok(())
    }

    /// `manifest_process_file` — invoked for each file mentioned in the backup
    /// manifest. We store the path and size for sanity-checking purposes (the
    /// checksum fields are discarded, exactly as in C).
    /// (basebackup_incremental.c:977-995)
    fn manifest_process_file(&mut self, record: &ManifestFileRecord) {
        let _ = record.checksum_type; // pg_checksum_type checksum_type (unused, as in C)
        let _ = record.checksum_length; // int checksum_length (unused)
        let _ = &record.checksum_payload; // uint8 *checksum_payload (unused)
        self.manifest_files
            .entry(record.pathname.clone())
            .or_insert_with(|| BackupFileEntry {
                path: record.pathname.clone(),
                size: record.size,
            });
    }

    /// `manifest_process_wal_range` — invoked for each WAL range mentioned in
    /// the backup manifest. (basebackup_incremental.c:1004-1016)
    fn manifest_process_wal_range(
        &mut self,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) {
        self.manifest_wal_ranges.push(BackupWalRange {
            tli,
            start_lsn,
            end_lsn,
        });
    }
}

/// `CreateIncrementalBackupInfo(MemoryContext mcxt)` — free-function form
/// mirroring the C entry point. (basebackup_incremental.c:151-186)
pub fn CreateIncrementalBackupInfo<'mcx>(mcx: Mcx<'mcx>) -> IncrementalBackupInfo<'mcx> {
    IncrementalBackupInfo::new(mcx)
}

/// `GetIncrementalFilePath(dboid, spcoid, relfilenumber, forknum, segno)`.
///
/// The result is a palloc'd string in C; here it's an owned `String`.
/// (basebackup_incremental.c:624-645)
pub fn GetIncrementalFilePath(
    dboid: Oid,
    spcoid: Oid,
    relfilenumber: RelFileNumber,
    forknum: ForkNumber,
    segno: u32,
) -> String {
    let path = GetRelationPath(dboid, spcoid, relfilenumber, INVALID_PROC_NUMBER, forknum);

    // lastslash = strrchr(path.str, '/'); Assert(lastslash != NULL);
    // *lastslash = '\0';
    let slash = path
        .rfind('/')
        .expect("GetRelationPath always contains a directory separator");
    let dir = &path[..slash];
    let base = &path[slash + 1..];

    if segno > 0 {
        format!("{dir}/INCREMENTAL.{base}.{segno}")
    } else {
        format!("{dir}/INCREMENTAL.{base}")
    }
}

/// `GetIncrementalHeaderSize(num_blocks_required)`.
///
/// Compute the size for a header of an incremental file containing a given
/// number of blocks. The header is rounded to a multiple of BLCKSZ, but only if
/// the file will store some block data. (basebackup_incremental.c:880-903)
pub fn GetIncrementalHeaderSize(num_blocks_required: u32) -> usize {
    // Make sure we're not going to overflow.
    assert!(num_blocks_required <= RELSEG_SIZE);

    // Three four byte quantities (magic number, truncation block length, block
    // count) followed by block numbers.
    let mut result: usize = 3 * core::mem::size_of::<u32>()
        + (core::mem::size_of::<BlockNumber>() * num_blocks_required as usize);

    // Round the header size to a multiple of BLCKSZ - when not a multiple of
    // BLCKSZ, add the missing fraction of a block. But do this only if the file
    // will store data for some blocks, otherwise keep it small.
    if (num_blocks_required > 0) && (result % BLCKSZ != 0) {
        result += BLCKSZ - (result % BLCKSZ);
    }

    result
}

/// `GetIncrementalFileSize(num_blocks_required)`.
///
/// Compute the size for an incremental file containing a given number of
/// blocks. (basebackup_incremental.c:908-925)
pub fn GetIncrementalFileSize(num_blocks_required: u32) -> usize {
    // Make sure we're not going to overflow.
    assert!(num_blocks_required <= RELSEG_SIZE);

    // Header with three four byte quantities (magic number, truncation block
    // length, block count) followed by block numbers, rounded to a multiple of
    // BLCKSZ (for files with block data), followed by block contents.
    let mut result = GetIncrementalHeaderSize(num_blocks_required);
    result += BLCKSZ * num_blocks_required as usize;

    result
}

/// `manifest_report_error(context, fmt, ...)` — raise `ERROR` with the
/// formatted message via `errmsg_internal`. The variadic format is rendered by
/// the caller. Never returns (always an `Err`).
/// (basebackup_incremental.c:1022-1044)
fn manifest_report_error<T>(message: impl Into<String>) -> PgResult<T> {
    ereport(ERROR)
        .errmsg_internal(message)
        .finish(here("manifest_report_error"))
        .map(|()| unreachable!("ERROR always returns Err"))
}

/// `compare_block_numbers(a, b)` — quicksort comparator for block numbers.
/// `pg_cmp_u32` is `(a > b) - (a < b)` (common/int.h), the unsigned three-way
/// comparison. (basebackup_incremental.c:1049-1056)
fn compare_block_numbers(aa: BlockNumber, bb: BlockNumber) -> i32 {
    (aa > bb) as i32 - (aa < bb) as i32
}

/// Accumulate the records decoded by one streaming-parser step (`dst`) into the
/// running document-order record set (`acc`). The version / system-identifier
/// fields appear at most once in the whole manifest, so a later chunk only
/// overwrites them if it actually decoded one (`Some`); per-file / per-WAL-range
/// records append.
fn merge_parsed(acc: &mut ParsedManifestChunk, dst: ParsedManifestChunk) {
    if dst.version.is_some() {
        acc.version = dst.version;
    }
    if dst.system_identifier.is_some() {
        acc.system_identifier = dst.system_identifier;
    }
    acc.files.extend(dst.files);
    acc.wal_ranges.extend(dst.wal_ranges);
}

/// Install this crate's seams. This unit declares no inward seams (its sole
/// consumer, `basebackup.c`, is a direct caller when it lands, like the other
/// `basebackup_*` leaf units), so there is nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
