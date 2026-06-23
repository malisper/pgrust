//! Port of PostgreSQL `src/backend/backup/walsummary.c` — functions for
//! accessing and managing WAL summary data.
//!
//! Each WAL summary file lives in `pg_wal/summaries` and its name encodes the
//! timeline plus the `[start_lsn, end_lsn)` LSN range it covers as five `%08X`
//! hex words (`TTTTTTTTSSSSSSSSssssssssEEEEEEEEeeeeeeee.summary`). The
//! functions here:
//!
//!  * enumerate the directory and decode each filename into a
//!    [`WalSummaryFile`], filtering by `(tli, start_lsn, end_lsn)`
//!    ([`get_wal_summaries`]);
//!  * re-filter an existing list by the same criteria
//!    ([`filter_wal_summaries`]);
//!  * decide whether a list of summaries fully covers an LSN range, locating
//!    the first gap if not ([`wal_summaries_are_complete`]);
//!  * open a summary file by name ([`open_wal_summary_file`]);
//!  * remove a stale summary file older than a cutoff
//!    ([`remove_wal_summary_if_older_than`]);
//!  * write a summary's bytes through the block-ref-table writer callback
//!    ([`write_wal_summary`]); and
//!  * set up the block-ref-table reader over a summary file, building the
//!    `ReadWalSummary` read callback and handing it to the blkreftable owner
//!    ([`wal_summary_create_reader`]).
//!
//! # Ported logic vs. seamed externals
//!
//! All of the computable logic is ported 1:1 over owned values: the filename
//! recognizer [`is_wal_summary_filename`], the hex parse/format helpers, the
//! scan-time and filter-time range predicates (note the strict vs. non-strict
//! comparison difference between `get_wal_summaries` and
//! `filter_wal_summaries`, preserved exactly as in C), the completeness walk
//! over a sorted private copy, the cutoff comparison, and the comparator.
//!
//! Only the genuine file/directory externals are seamed: the directory walk
//! (`AllocateDir`/`ReadDir`/`FreeDir` → `read_dir_names`), the `lstat`
//! modification-time probe (`lstat_mtime`), `PathNameOpenFile`
//! (`path_name_open_file`), the positional `FileRead` / `FileWrite`
//! (`file_read` / `file_write`), `FileClose` (`file_close`), `FilePathName`
//! (`file_path_name`) and `unlink` (`unlink_file`) — all from the fd owner.
//!
//! # Reader bundling
//!
//! `pg_wal_summary_contents` (walsummaryfuncs.c) opens a summary file and wraps
//! it in a `BlockRefTableReader` whose `io_callback` is `ReadWalSummary` over a
//! `WalSummaryIO` cursor (`{ file, filepos }`). The open + reader construction
//! is bundled into the [`wal_summary_create_reader`] owner seam: it opens the
//! `File`, builds the `ReadWalSummary` read-callback closure over the cursor,
//! and hands it directly to the blkreftable owner's
//! `common_blkreftable::create_block_ref_table_reader` (a plain pub fn — the C
//! `CreateBlockRefTableReader`), which returns the owned `BlockRefTableReader`.
//! blkreftable assembles and raises its own corruption `ereport(ERROR)`
//! (subsuming C's `ReportWalSummaryError`), keyed by the `error_filename` this
//! owner passes. The seam returns `(BlockRefTableReader, File)`: the open `File`
//! (a `Copy` VFD descriptor captured by the read callback) is threaded back by
//! the caller to the matching `FileClose` ([`wal_summary_reader_file_close`]),
//! so no side registry is needed.

use mcx::{Mcx, PgVec};

use utils_error::ereport;
use utils_error::errno::sqlstate_for_file_access;
use types_blkreftable::BlockRefTableReader;
use types_core::{InvalidXLogRecPtr, TimeLineID, XLogRecPtr};
use types_error::{ErrorLocation, PgError, PgResult, DEBUG2};
use types_pgstat::wait_event::{WAIT_EVENT_WAL_SUMMARY_READ, WAIT_EVENT_WAL_SUMMARY_WRITE};
use types_storage::file::File;
use types_walsummarizer::WalSummaryFile;

use walsummary_seams as walsummary_seams;
use fd_seams as fd;
use common_blkreftable as blkreftable_owner;

/// `XLOGDIR "/summaries"` — the directory enumerated and addressed by name
/// throughout this module.
const SUMMARY_DIR: &str = "pg_wal/summaries";

/// Length of the hex-encoded portion of a summary filename
/// (`tli` + start_lsn-hi/lo + end_lsn-hi/lo, 5 × 8 hex digits).
const SUMMARY_HEX_LEN: usize = 40;

/// `.summary` suffix that follows the 40 hex digits.
const SUMMARY_SUFFIX: &str = ".summary";

/// `ENOENT` — the only errno value the open path distinguishes.
const ENOENT: i32 = libc::ENOENT;

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/backup/walsummary.c", 0, funcname)
}

// ---------------------------------------------------------------------------
// WAL-summary I/O state.
//
// `WalSummaryIO { File file; off_t filepos; }` (backup/walsummary.h) — the
// write-path callback arg (`WriteWalSummary`'s `io->filepos += nbytes`). On the
// read path the cursor is captured directly by the `ReadWalSummary` closure in
// `wal_summary_create_reader` (the reader is an owned `BlockRefTableReader`, not
// an opaque handle, so no side registry is needed).
// ---------------------------------------------------------------------------

/// `WalSummaryIO` (backup/walsummary.h).
#[derive(Clone, Copy, Debug)]
struct WalSummaryIo {
    /// `io->file` — the open VFD of the summary file.
    file: File,
    /// `io->filepos` — the read/write cursor advanced by each callback.
    filepos: i64,
}

// ---------------------------------------------------------------------------
// GetWalSummaries (walsummary.c:53).
// ---------------------------------------------------------------------------

/// Get a list of WAL summaries.
///
/// If `tli != 0`, only WAL summaries with the indicated TLI will be included.
///
/// If `start_lsn != InvalidXLogRecPtr`, only summaries that end after the
/// indicated LSN will be included.
///
/// If `end_lsn != InvalidXLogRecPtr`, only summaries that start before the
/// indicated LSN will be included.
pub fn get_wal_summaries<'mcx>(
    mcx: Mcx<'mcx>,
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) -> PgResult<PgVec<'mcx, WalSummaryFile>> {
    // List *result = NIL;
    let mut result: PgVec<'mcx, WalSummaryFile> = PgVec::new_in(mcx);

    // sdir = AllocateDir(XLOGDIR "/summaries");
    // while ((dent = ReadDir(sdir, XLOGDIR "/summaries")) != NULL) { ... }
    // FreeDir(sdir);
    for name in fd::read_dir_names::call(SUMMARY_DIR)? {
        // Decode filename, or skip if it's not in the expected format.
        // if (!IsWalSummaryFilename(dent->d_name)) continue;
        if !is_wal_summary_filename(&name) {
            continue;
        }
        // sscanf(dent->d_name, "%08X%08X%08X%08X%08X", &tmp[0..4]);
        let tmp0 = parse_hex_u32(&name[0..8]);
        let tmp1 = parse_hex_u32(&name[8..16]);
        let tmp2 = parse_hex_u32(&name[16..24]);
        let tmp3 = parse_hex_u32(&name[24..32]);
        let tmp4 = parse_hex_u32(&name[32..40]);
        // file_tli = tmp[0];
        let file_tli: TimeLineID = tmp0;
        // file_start_lsn = ((uint64) tmp[1]) << 32 | tmp[2];
        let file_start_lsn: XLogRecPtr = ((tmp1 as u64) << 32) | tmp2 as u64;
        // file_end_lsn = ((uint64) tmp[3]) << 32 | tmp[4];
        let file_end_lsn: XLogRecPtr = ((tmp3 as u64) << 32) | tmp4 as u64;

        // Skip if it doesn't match the filter criteria.
        // if (tli != 0 && tli != file_tli) continue;
        if tli != 0 && tli != file_tli {
            continue;
        }
        // if (!XLogRecPtrIsInvalid(start_lsn) && start_lsn >= file_end_lsn) continue;
        if !xlog_rec_ptr_is_invalid(start_lsn) && start_lsn >= file_end_lsn {
            continue;
        }
        // if (!XLogRecPtrIsInvalid(end_lsn) && end_lsn <= file_start_lsn) continue;
        if !xlog_rec_ptr_is_invalid(end_lsn) && end_lsn <= file_start_lsn {
            continue;
        }

        // ws = palloc(...); ws->tli/start_lsn/end_lsn = ...;
        // result = lappend(result, ws);
        result.push(WalSummaryFile {
            tli: file_tli,
            start_lsn: file_start_lsn,
            end_lsn: file_end_lsn,
        });
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// FilterWalSummaries (walsummary.c:103).
// ---------------------------------------------------------------------------

/// Build a new list of WAL summaries based on an existing list, but filtering
/// out summaries that don't match the search parameters.
///
/// If `tli != 0`, only WAL summaries with the indicated TLI will be included.
///
/// If `start_lsn != InvalidXLogRecPtr`, only summaries that end after the
/// indicated LSN will be included.
///
/// If `end_lsn != InvalidXLogRecPtr`, only summaries that start before the
/// indicated LSN will be included.
pub fn filter_wal_summaries<'mcx>(
    mcx: Mcx<'mcx>,
    wslist: &[WalSummaryFile],
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) -> PgResult<PgVec<'mcx, WalSummaryFile>> {
    // List *result = NIL;
    let mut result: PgVec<'mcx, WalSummaryFile> = PgVec::new_in(mcx);

    // foreach(lc, wslist) { WalSummaryFile *ws = lfirst(lc); ... }
    for ws in wslist {
        // Skip if it doesn't match the filter criteria.
        // if (tli != 0 && tli != ws->tli) continue;
        if tli != 0 && tli != ws.tli {
            continue;
        }
        // if (!XLogRecPtrIsInvalid(start_lsn) && start_lsn > ws->end_lsn) continue;
        if !xlog_rec_ptr_is_invalid(start_lsn) && start_lsn > ws.end_lsn {
            continue;
        }
        // if (!XLogRecPtrIsInvalid(end_lsn) && end_lsn < ws->start_lsn) continue;
        if !xlog_rec_ptr_is_invalid(end_lsn) && end_lsn < ws.start_lsn {
            continue;
        }

        // result = lappend(result, ws);
        result.push(*ws);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// WalSummariesAreComplete (walsummary.c:144).
// ---------------------------------------------------------------------------

/// Check whether the supplied list of [`WalSummaryFile`] objects covers the
/// whole range of LSNs from `start_lsn` to `end_lsn`. This function ignores
/// timelines, so the caller should probably filter using the appropriate
/// timeline before calling this.
///
/// If the whole range of LSNs is covered, returns `true`, otherwise `false`.
/// If `false` is returned, `*missing_lsn` is set either to `InvalidXLogRecPtr`
/// if there are no WAL summary files in the input list, or to the first LSN in
/// the range that is not covered by a WAL summary file in the input list.
pub fn wal_summaries_are_complete(
    wslist: &[WalSummaryFile],
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    missing_lsn: &mut XLogRecPtr,
) -> bool {
    // XLogRecPtr current_lsn = start_lsn;
    let mut current_lsn = start_lsn;

    // Special case for empty list.
    // if (wslist == NIL) { *missing_lsn = InvalidXLogRecPtr; return false; }
    if wslist.is_empty() {
        *missing_lsn = InvalidXLogRecPtr;
        return false;
    }

    // Make a private copy of the list and sort it by start LSN.
    // wslist = list_copy(wslist);
    // list_sort(wslist, ListComparatorForWalSummaryFiles);
    let mut wslist = wslist.to_vec();
    wslist.sort_by(|a, b| {
        list_comparator_for_wal_summary_files(a, b).cmp(&0)
    });

    // Consider summary files in order of increasing start_lsn, advancing the
    // known-summarized range from start_lsn toward end_lsn.
    //
    // Normally, the summary files should cover non-overlapping WAL ranges, but
    // this algorithm is intended to be correct even in case of overlap.
    for ws in &wslist {
        // if (ws->start_lsn > current_lsn) break;  /* We found a gap. */
        if ws.start_lsn > current_lsn {
            break;
        }
        // if (ws->end_lsn > current_lsn) { ... }
        if ws.end_lsn > current_lsn {
            // Next summary extends beyond end of previous summary, so extend the
            // end of the range known to be summarized.
            current_lsn = ws.end_lsn;

            // If the range we know to be summarized has reached the required
            // end LSN, we have proved completeness.
            if current_lsn >= end_lsn {
                return true;
            }
        }
    }

    // We either ran out of summary files without reaching the end LSN, or we
    // hit a gap in the sequence that resulted in us bailing out of the loop
    // above.
    *missing_lsn = current_lsn;
    false
}

// ---------------------------------------------------------------------------
// OpenWalSummaryFile (walsummary.c:204).
// ---------------------------------------------------------------------------

/// Open a WAL summary file.
///
/// This will throw an error in case of trouble. As an exception, if
/// `missing_ok = true` and the trouble is specifically that the file does not
/// exist, it will not throw an error and will return `None` (the C function
/// returns a `File` value less than 0).
pub fn open_wal_summary_file(ws: &WalSummaryFile, missing_ok: bool) -> PgResult<Option<File>> {
    // snprintf(path, MAXPGPATH, XLOGDIR "/summaries/%08X..summary", ...);
    let path = wal_summary_path(ws);

    // file = PathNameOpenFile(path, O_RDONLY);
    let file = fd::path_name_open_file::call(&path, libc::O_RDONLY)?;

    // if (file < 0 && (errno != ENOENT || !missing_ok))
    //     ereport(ERROR, errcode_for_file_access, "could not open file ...");
    if file.0 < 0 {
        let errno = fd::last_errno::call();
        if errno != ENOENT || !missing_ok {
            return Err(file_access_error("could not open file", &path, errno));
        }
        // missing_ok && ENOENT: return the negative File (None).
        return Ok(None);
    }

    // return file;
    Ok(Some(file))
}

// ---------------------------------------------------------------------------
// RemoveWalSummaryIfOlderThan (walsummary.c:227).
// ---------------------------------------------------------------------------

/// Remove a WAL summary file if the last modification time precedes the cutoff
/// time.
pub fn remove_wal_summary_if_older_than(
    ws: WalSummaryFile,
    cutoff_time: i64,
) -> PgResult<()> {
    // snprintf(path, MAXPGPATH, XLOGDIR "/summaries/%08X..summary", ...);
    let path = wal_summary_path(&ws);

    // if (lstat(path, &statbuf) != 0) {
    //     if (errno == ENOENT) return;
    //     ereport(ERROR, errcode_for_file_access, "could not stat file ...");
    // }
    let st_mtime = match fd::lstat_mtime::call(&path)? {
        None => return Ok(()),
        Some(mtime) => mtime,
    };

    // if (statbuf.st_mtime >= cutoff_time) return;
    if st_mtime >= cutoff_time {
        return Ok(());
    }

    // if (unlink(path) != 0)
    //     ereport(ERROR, errcode_for_file_access, "could not remove file ...");
    let rc = fd::unlink_file::call(&path);
    if rc != 0 {
        // The seam returns `-errno` on failure.
        return Err(file_access_error("could not remove file", &path, -rc));
    }

    // ereport(DEBUG2, errmsg_internal("removing file \"%s\"", path));
    ereport(DEBUG2)
        .errmsg_internal(format!("removing file \"{path}\""))
        .finish(loc("RemoveWalSummaryIfOlderThan"))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// IsWalSummaryFilename (walsummary.c:259).
// ---------------------------------------------------------------------------

/// Test whether a filename looks like a WAL summary file.
///
/// Mirrors C `IsWalSummaryFilename`: `strspn(filename, "0123456789ABCDEF") ==
/// 40 && strcmp(filename + 40, ".summary") == 0`. The hex alphabet is
/// upper-case only (matching the `%08X` formatter), so a lower-case hex digit
/// makes the name invalid.
pub fn is_wal_summary_filename(filename: &str) -> bool {
    let bytes = filename.as_bytes();
    // strspn over the upper-case hex alphabet.
    let span = bytes
        .iter()
        .take_while(|&&b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
        .count();
    span == SUMMARY_HEX_LEN
        && filename.len() >= SUMMARY_HEX_LEN
        && &filename[SUMMARY_HEX_LEN..] == SUMMARY_SUFFIX
}

// ---------------------------------------------------------------------------
// ReadWalSummary (walsummary.c:269).
//
// The C `io_callback_fn` `ReadWalSummary` is no longer a separate installed
// seam: blkreftable's `CreateBlockRefTableReader` takes the read callback
// directly (`common_blkreftable::create_block_ref_table_reader`), so the cursor
// read is built inline as the `read_callback` closure in
// `wal_summary_create_reader` (advancing the captured `filepos` exactly as
// `io->filepos += nbytes`).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// WriteWalSummary (walsummary.c:288).
// ---------------------------------------------------------------------------

/// Data write callback for use with `WriteBlockRefTable` (`WriteWalSummary`).
/// Writes `data` at `io.filepos`, advancing it by the count returned and
/// returning the updated cursor. A write error (the C `nbytes < 0`) or a short
/// write (the C `nbytes != length`) is reported.
fn write_wal_summary(io: &mut WalSummaryIo, data: &[u8]) -> PgResult<usize> {
    // int length = ... (the amount requested)
    let length = data.len();

    // nbytes = FileWrite(io->file, data, length, io->filepos,
    //                    WAIT_EVENT_WAL_SUMMARY_WRITE);
    let nbytes = fd::file_write::call(io.file, data, io.filepos, WAIT_EVENT_WAL_SUMMARY_WRITE)?;

    // if (nbytes < 0)
    //     ereport(ERROR, errcode_for_file_access, "could not write file ...");
    if nbytes < 0 {
        let path = fd::file_path_name::call(io.file);
        return Err(file_access_error("could not write file", &path, fd::last_errno::call()));
    }
    let nbytes = nbytes as usize;

    // if (nbytes != length)
    //     ereport(ERROR, errcode_for_file_access,
    //         "could not write file ...: wrote only %d of %d bytes at offset %u",
    //         errhint("Check free disk space."));
    if nbytes != length {
        let path = fd::file_path_name::call(io.file);
        // C uses errcode_for_file_access(), which reads the saved errno (the
        // short write itself left errno set, typically ENOSPC).
        let errno = fd::last_errno::call();
        return Err(PgError::error(format!(
            "could not write file \"{path}\": wrote only {nbytes} of {length} bytes at offset {}",
            io.filepos as u32
        ))
        .with_saved_errno(errno)
        .with_sqlstate(sqlstate_for_file_access(errno))
        .with_hint("Check free disk space."));
    }

    // io->filepos += nbytes;
    io.filepos += nbytes as i64;

    // return nbytes;
    Ok(nbytes)
}

// ---------------------------------------------------------------------------
// ReportWalSummaryError (walsummary.c:315).
//
// The C `report_error_fn` `ReportWalSummaryError` is no longer a separate
// installed seam: blkreftable's reader assembles and raises its own
// `ereport(ERROR)` (`report_error`, keyed by the `error_filename` passed to
// `create_block_ref_table_reader`), folding in the corruption message C built
// from the printf-style format. No error callback is threaded from this owner.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ListComparatorForWalSummaryFiles (walsummary.c:336).
// ---------------------------------------------------------------------------

/// Comparator to sort a list of [`WalSummaryFile`] objects by `start_lsn`.
///
/// Mirrors C `ListComparatorForWalSummaryFiles` (`pg_cmp_u64`).
fn list_comparator_for_wal_summary_files(ws1: &WalSummaryFile, ws2: &WalSummaryFile) -> i32 {
    pg_cmp_u64(ws1.start_lsn, ws2.start_lsn)
}

// ---------------------------------------------------------------------------
// Bundled owner seams driven by walsummaryfuncs / SummarizeWAL.
// ---------------------------------------------------------------------------

/// `wal_summary_create_reader` — the `pg_wal_summary_contents` reader setup:
/// `OpenWalSummaryFile(&ws, false)` followed by
/// `CreateBlockRefTableReader(ReadWalSummary, &io, FilePathName(io.file),
/// ReportWalSummaryError, NULL)`. Opens the summary file, builds the
/// `ReadWalSummary` `io_callback` over the file's `WalSummaryIO` cursor, and
/// hands it to the blkreftable owner's `CreateBlockRefTableReader`
/// (`common_blkreftable::create_block_ref_table_reader`) which mints and returns
/// the reader handle after verifying the magic number. The open `File` is then
/// recorded in this owner's registry keyed by that handle so the matching
/// `FileClose` teardown can find it.
///
/// The C `read_callback_arg` is `&io` (the `WalSummaryIO` cursor); here the
/// cursor (`file` + `filepos`) is captured directly by the `io_callback`
/// closure, which advances `filepos` on each read. The C `error_callback`
/// (`ReportWalSummaryError`) is folded into the blkreftable owner's own
/// `report_error` `ereport(ERROR)` assembly (keyed by `error_filename`), so no
/// separate error callback is threaded.
fn wal_summary_create_reader<'mcx>(
    _mcx: Mcx<'mcx>,
    ws: WalSummaryFile,
) -> PgResult<(BlockRefTableReader, File)> {
    // io.filepos = 0;
    // io.file = OpenWalSummaryFile(&ws, false);
    let file = match open_wal_summary_file(&ws, false)? {
        // missing_ok is false here, so a missing file would have been an Err.
        Some(file) => file,
        None => unreachable!("OpenWalSummaryFile(missing_ok=false) returned None"),
    };
    // FilePathName(io.file) — supplies the "%s" of blkreftable's error messages.
    let error_filename = fd::file_path_name::call(file);

    // Build the ReadWalSummary read callback over the WalSummaryIO cursor
    // (`{ File file; off_t filepos; }`). The closure owns the cursor: `file`
    // (a Copy VFD handle) and a mutable `filepos` advanced by each read, exactly
    // as C's `io->filepos += nbytes`.
    //
    //   static int ReadWalSummary(void *callback_arg, void *data, int length) {
    //       WalSummaryIO *io = callback_arg;
    //       nbytes = FileRead(io->file, data, length, io->filepos,
    //                         WAIT_EVENT_WAL_SUMMARY_READ);
    //       if (nbytes < 0) ereport(ERROR, ... "could not read file ...");
    //       io->filepos += nbytes;
    //       return nbytes;
    //   }
    //
    // FileRead's `Err` / negative-count error path is the C `ereport(ERROR)`
    // longjmp; the `io_callback_fn` contract returns a plain byte count, so a
    // read failure surfaces as a zero-byte read, which blkreftable's buffered
    // reader turns into the `"file \"%s\" ends unexpectedly"` `ereport(ERROR)`
    // (same fatal outcome, aborting the SRF).
    let mut filepos: i64 = 0;
    let read_callback: blkreftable_owner::ReadCallback = Box::new(move |data: &mut [u8]| {
        match fd::file_read::call(file, data, filepos, WAIT_EVENT_WAL_SUMMARY_READ) {
            Ok(nbytes) if nbytes >= 0 => {
                let nbytes = nbytes as usize;
                filepos += nbytes as i64;
                nbytes
            }
            // nbytes < 0 (OS read error) or a hard fd.c ereport(ERROR): the C
            // path would ereport(ERROR); signal EOF so the reader raises its own
            // corruption ereport.
            _ => 0,
        }
    });

    // reader = CreateBlockRefTableReader(ReadWalSummary, &io,
    //                                    FilePathName(io.file),
    //                                    ReportWalSummaryError, NULL);
    match blkreftable_owner::create_block_ref_table_reader(read_callback, error_filename) {
        // Return the owned reader plus the open File (a Copy VFD descriptor):
        // the reader's ReadWalSummary callback already captured a copy of `file`
        // for its reads; the caller threads this returned `file` to the matching
        // FileClose teardown (wal_summary_reader_file_close).
        Ok(reader) => Ok((reader, file)),
        Err(e) => {
            // The reader was not constructed; close the file we opened so it
            // does not leak (the C path FileCloses on the error too).
            fd::file_close::call(file);
            Err(e)
        }
    }
}

/// `wal_summary_reader_file_close` — the `pg_wal_summary_contents` reader
/// teardown: after `DestroyBlockRefTableReader(reader)` (blkreftable-owned),
/// `FileClose(io.file)` closes the WAL summary file the reader was reading. The
/// open `File` is the one returned by [`wal_summary_create_reader`], threaded
/// back by the caller.
fn wal_summary_reader_file_close(file: File) {
    // FileClose(io.file);
    fd::file_close::call(file);
}

// ---------------------------------------------------------------------------
// Path / filename helpers (ported from the in-line snprintf in walsummary.c).
// ---------------------------------------------------------------------------

/// Format the bare summary filename for `ws`:
/// `"%08X%08X%08X%08X%08X.summary"` over `tli`, `LSN_FORMAT_ARGS(start_lsn)`,
/// `LSN_FORMAT_ARGS(end_lsn)`.
pub fn wal_summary_filename(ws: &WalSummaryFile) -> String {
    format!(
        "{:08X}{:08X}{:08X}{:08X}{:08X}.summary",
        ws.tli,
        (ws.start_lsn >> 32) as u32,
        ws.start_lsn as u32,
        (ws.end_lsn >> 32) as u32,
        ws.end_lsn as u32,
    )
}

/// Build the full `XLOGDIR "/summaries/<filename>"` path used by the open /
/// lstat / unlink seams (`snprintf(path, MAXPGPATH, XLOGDIR "/summaries/..."`).
pub fn wal_summary_path(ws: &WalSummaryFile) -> String {
    format!("{}/{}", SUMMARY_DIR, wal_summary_filename(ws))
}

// ---------------------------------------------------------------------------
// Small numeric / validity helpers mirroring the C macros.
// ---------------------------------------------------------------------------

/// `XLogRecPtrIsInvalid(r)` (access/xlogdefs.h) == `(r) == InvalidXLogRecPtr`.
#[inline]
fn xlog_rec_ptr_is_invalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// `pg_cmp_u64(a, b)` (common/int.h): `(a > b) - (a < b)`.
#[inline]
fn pg_cmp_u64(a: u64, b: u64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

/// Parse exactly 8 upper-case hex digits to a `u32`. Callers only invoke this
/// after [`is_wal_summary_filename`] has confirmed the 40-char hex prefix, so
/// the slice is always valid hex (the C `sscanf` likewise assumes this).
#[inline]
fn parse_hex_u32(s: &str) -> u32 {
    u32::from_str_radix(s, 16).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Error construction helper mirroring the `ereport(ERROR,
// (errcode_for_file_access(), errmsg("could not ... \"%s\": %m", path)))`
// reports in walsummary.c.
// ---------------------------------------------------------------------------

fn file_access_error(action: &str, path: &str, errno: i32) -> PgError {
    PgError::error(format!("{action} \"{path}\": %m"))
        .with_saved_errno(errno)
        .with_sqlstate(sqlstate_for_file_access(errno))
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this unit's inward seams.
pub fn init_seams() {
    walsummary_seams::get_wal_summaries::set(get_wal_summaries);
    walsummary_seams::remove_wal_summary_if_older_than::set(remove_wal_summary_if_older_than);
    walsummary_seams::write_wal_summary_file::set(write_wal_summary_file);
    walsummary_seams::wal_summary_create_reader::set(wal_summary_create_reader);
    walsummary_seams::wal_summary_reader_file_close::set(wal_summary_reader_file_close);
}

// ---------------------------------------------------------------------------
// write_wal_summary_file (the SummarizeWAL file-emit sequence).
// ---------------------------------------------------------------------------

/// `write_wal_summary_file` — the SummarizeWAL summary-emit file sequence:
/// `PathNameOpenFile(temp_path, O_WRONLY | O_CREAT | O_TRUNC)`,
/// `WriteBlockRefTable` streaming the already-serialized `bytes` through
/// [`write_wal_summary`], `FileClose`, then `durable_rename(temp_path,
/// final_path, ERROR)`. The caller hands the serialized bytes; this performs
/// the walsummary-owned open/write/close and the rename.
fn write_wal_summary_file(temp_path: &str, final_path: &str, bytes: &[u8]) -> PgResult<()> {
    // io.filepos = 0;
    // io.file = PathNameOpenFile(temp_path, O_WRONLY | O_CREAT | O_TRUNC);
    let file = fd::path_name_open_file::call(
        temp_path,
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
    )?;
    if file.0 < 0 {
        let errno = fd::last_errno::call();
        return Err(file_access_error("could not open file", temp_path, errno));
    }
    let mut io = WalSummaryIo { file, filepos: 0 };

    // WriteBlockRefTable(brtab, WriteWalSummary, &io); — stream the serialized
    // bytes through the write callback exactly once (the caller already
    // serialized the table to `bytes`).
    let write_result = write_wal_summary(&mut io, bytes).map(|_| ());

    // FileClose(io.file);
    fd::file_close::call(io.file);
    write_result?;

    // durable_rename(temp_path, final_path, ERROR);
    let rc = fd::rename_file::call(temp_path, final_path);
    if rc != 0 {
        let errno = -rc;
        return Err(PgError::error(format!(
            "could not rename file \"{temp_path}\" to \"{final_path}\": %m"
        ))
        .with_saved_errno(errno)
        .with_sqlstate(sqlstate_for_file_access(errno)));
    }

    Ok(())
}
