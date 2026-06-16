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
//!  * read / write a summary's bytes through the block-ref-table
//!    reader/writer callbacks ([`read_wal_summary`] / [`write_wal_summary`]);
//!    and
//!  * report a block-ref-table read error ([`report_wal_summary_error`]).
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
//! it in a `BlockRefTableReader` whose `io_callback` is [`read_wal_summary`]
//! and whose `error_callback` is [`report_wal_summary_error`], over a
//! `WalSummaryIO` cursor (`{ file, filepos }`). Because the open `File`, the
//! read callback and the error callback are all walsummary-owned while the
//! reader struct is blkreftable-owned, the open + reader construction is
//! bundled into the [`wal_summary_create_reader`] owner seam and the matching
//! `FileClose` into [`wal_summary_reader_file_close`]. This owner keeps the
//! per-reader `WalSummaryIO` in a registry keyed by the reader handle;
//! blkreftable reaches the cursor through the [`read_wal_summary`] /
//! [`report_wal_summary_error`] seams (also installed here) keyed by that same
//! handle, and `CreateBlockRefTableReader` itself is the
//! `common-blkreftable` `create_block_ref_table_reader` seam (owner not yet
//! ported: loud seam-and-panic).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use mcx::{Mcx, PgVec};

use backend_utils_error::ereport;
use backend_utils_error::errno::sqlstate_for_file_access;
use types_blkreftable::BlockRefTableReaderHandle;
use types_core::{InvalidXLogRecPtr, TimeLineID, XLogRecPtr};
use types_error::{ErrorLocation, PgError, PgResult, DEBUG2, ERRCODE_DATA_CORRUPTED, ERROR};
use types_pgstat::wait_event::{WAIT_EVENT_WAL_SUMMARY_READ, WAIT_EVENT_WAL_SUMMARY_WRITE};
use types_storage::file::File;
use types_walsummarizer::WalSummaryFile;

use backend_backup_walsummary_seams as walsummary_seams;
use backend_storage_file_fd_seams as fd;
use common_blkreftable_seams as blkreftable;

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
// Per-reader WAL-summary I/O state.
//
// `WalSummaryIO { File file; off_t filepos; }` (backup/walsummary.h) — the
// `read_callback_arg` for the block-reference-table reader. The reader struct
// is blkreftable-owned and opaque (a `BlockRefTableReaderHandle`), so this
// owner keeps the cursor in a registry keyed by that handle: the read / report
// / close seams resolve the handle back to its `WalSummaryIO`.
// ---------------------------------------------------------------------------

/// `WalSummaryIO` (backup/walsummary.h).
#[derive(Clone, Copy, Debug)]
struct WalSummaryIo {
    /// `io->file` — the open VFD of the summary file.
    file: File,
    /// `io->filepos` — the read/write cursor advanced by each callback.
    filepos: i64,
}

/// Registry of live `WalSummaryIO` cursors keyed by reader handle.
fn io_registry() -> &'static Mutex<HashMap<u64, WalSummaryIo>> {
    static REGISTRY: OnceLock<Mutex<HashMap<u64, WalSummaryIo>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Mint a fresh, never-reused reader handle.
fn next_reader_handle() -> BlockRefTableReaderHandle {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    BlockRefTableReaderHandle(NEXT.fetch_add(1, Ordering::Relaxed))
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
// ReadWalSummary (walsummary.c:269) — installed as `wal_summary_read`.
// ---------------------------------------------------------------------------

/// Data read callback for use with `CreateBlockRefTableReader`
/// (`ReadWalSummary`). Reads up to `length` bytes at the cursor's current
/// `filepos`, advancing it by the count returned. A read error (the C
/// `nbytes < 0`) is reported.
///
/// The cursor is resolved from the reader handle's registry entry.
pub fn read_wal_summary<'mcx>(
    mcx: Mcx<'mcx>,
    reader: BlockRefTableReaderHandle,
    length: usize,
) -> PgResult<PgVec<'mcx, u8>> {
    // io = wal_summary_io; (resolved from the registry)
    let (file, filepos) = {
        let registry = io_registry().lock().unwrap();
        let io = registry
            .get(&reader.0)
            .expect("read_wal_summary: unknown WAL summary reader handle");
        (io.file, io.filepos)
    };

    // nbytes = FileRead(io->file, data, length, io->filepos,
    //                   WAIT_EVENT_WAL_SUMMARY_READ);
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, length)?;
    buf.resize(length, 0u8);
    let nbytes = fd::file_read::call(file, &mut buf, filepos, WAIT_EVENT_WAL_SUMMARY_READ)?;

    // if (nbytes < 0)
    //     ereport(ERROR, errcode_for_file_access, "could not read file ...");
    if nbytes < 0 {
        let path = fd::file_path_name::call(file);
        return Err(file_access_error("could not read file", &path, fd::last_errno::call()));
    }
    let nbytes = nbytes as usize;

    // io->filepos += nbytes;
    {
        let mut registry = io_registry().lock().unwrap();
        if let Some(io) = registry.get_mut(&reader.0) {
            io.filepos += nbytes as i64;
        }
    }

    // return nbytes;  (the caller reads `nbytes` bytes of `data`)
    buf.truncate(nbytes);
    Ok(buf)
}

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
// ReportWalSummaryError (walsummary.c:315) — installed as
// `wal_summary_report_error`.
// ---------------------------------------------------------------------------

/// Error-reporting callback for use with `CreateBlockRefTableReader`
/// (`ReportWalSummaryError`). The C function assembles a message from a
/// printf-style format and varargs; in the owned model blkreftable assembles
/// the message and passes it here. Always raises `ereport(ERROR,
/// errcode(ERRCODE_DATA_CORRUPTED), errmsg_internal("%s", buf))`.
pub fn report_wal_summary_error(
    _reader: BlockRefTableReaderHandle,
    message: &str,
) -> PgResult<()> {
    // ereport(ERROR, errcode(ERRCODE_DATA_CORRUPTED), errmsg_internal("%s", buf.data));
    ereport(ERROR)
        .errcode(ERRCODE_DATA_CORRUPTED)
        .errmsg_internal(message.to_owned())
        .finish(loc("ReportWalSummaryError"))
}

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
/// ReportWalSummaryError, NULL)`. Opens the summary file, mints a reader handle
/// and registers its `WalSummaryIO` cursor, then asks blkreftable to construct
/// the reader (which verifies the magic via the [`read_wal_summary`] /
/// [`report_wal_summary_error`] callbacks keyed by the handle).
fn wal_summary_create_reader<'mcx>(
    mcx: Mcx<'mcx>,
    ws: WalSummaryFile,
) -> PgResult<BlockRefTableReaderHandle> {
    // io.filepos = 0;
    // io.file = OpenWalSummaryFile(&ws, false);
    let file = match open_wal_summary_file(&ws, false)? {
        // missing_ok is false here, so a missing file would have been an Err.
        Some(file) => file,
        None => unreachable!("OpenWalSummaryFile(missing_ok=false) returned None"),
    };
    let error_filename = fd::file_path_name::call(file);

    // Mint the reader handle and register the cursor before constructing the
    // reader, so blkreftable's initial magic read can resolve it.
    let reader = next_reader_handle();
    io_registry()
        .lock()
        .unwrap()
        .insert(reader.0, WalSummaryIo { file, filepos: 0 });

    // reader = CreateBlockRefTableReader(ReadWalSummary, &io,
    //                                    FilePathName(io.file),
    //                                    ReportWalSummaryError, NULL);
    match blkreftable::create_block_ref_table_reader::call(mcx, reader, &error_filename) {
        Ok(()) => Ok(reader),
        Err(e) => {
            // The reader was not constructed; drop the cursor and close the
            // file we opened so it does not leak.
            if let Some(io) = io_registry().lock().unwrap().remove(&reader.0) {
                fd::file_close::call(io.file);
            }
            Err(e)
        }
    }
}

/// `wal_summary_reader_file_close` — the `pg_wal_summary_contents` reader
/// teardown: after `DestroyBlockRefTableReader(reader)` (blkreftable-owned),
/// `FileClose(io.file)` closes the WAL summary file the reader was reading. The
/// open `File` lives in this owner's registry keyed by the reader handle.
fn wal_summary_reader_file_close(reader: BlockRefTableReaderHandle) {
    // FileClose(io.file);
    if let Some(io) = io_registry().lock().unwrap().remove(&reader.0) {
        fd::file_close::call(io.file);
    }
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
    walsummary_seams::wal_summary_read::set(read_wal_summary);
    walsummary_seams::wal_summary_report_error::set(report_wal_summary_error);
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
