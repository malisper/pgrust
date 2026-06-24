//! `access/transam/timeline.c` — reading and writing timeline-history files.
//!
//! A timeline-history file (`NNNNNNNN.history` in `pg_wal`) lists the timeline
//! changes of a timeline in a simple tab-separated text format; each
//! non-comment, non-empty line `<parentTLI>\t<switchpoint>\t<reason>` records a
//! timeline switch. This crate owns the parsing, the in-memory history lookups
//! ([`tliInHistory`], [`tliOfPointInHistory`], [`tliSwitchPoint`]), the
//! file-name/path formatting ([`TLHistoryFileName`]/[`TLHistoryFilePath`]), the
//! archive-vs-`pg_wal` branching, and the byte-exact assembly of a new history
//! file.
//!
//! The genuinely external operations cross per-owner seams:
//!   - archive restore / keep / notify -> `backend-access-transam-xlogarchive`;
//!   - the *individual* fd.c primitives the write path orchestrates —
//!     `OpenTransientFile` / `pg_fsync` / `CloseTransientFile` /
//!     `durable_rename` / `data_sync_elevel` -> `backend-storage-file`; the
//!     temp-name (`getpid`), `unlink`, and the `read`/`write`/`access` syscalls
//!     are plain libc, exactly as timeline.c open-codes them. The temp-file
//!     emplacement orchestration of `writeTimeLineHistory` /
//!     `writeTimeLineHistoryFile` lives in this crate, not behind a bundled
//!     seam;
//!   - the read/exists probes (`AllocateFile`+read / `AllocateFile`+`FreeFile`)
//!     -> `backend-storage-file-fd` (`read_file_or_absent` / `file_exists`).
//!
//! The `ArchiveRecoveryRequested` and `XLogArchivingActive()` per-backend
//! globals (owned by xlogrecovery / xlog) are passed in as explicit parameters,
//! not read through an ambient getter seam (AGENTS.md "No ambient-global
//! seams").
//!
//! `path`/`histfname`/the assembled line are C *stack* buffers (`char[MAXPGPATH]`
//! / `char[BLCKSZ]`, `snprintf`), not `palloc`; their Rust equivalents are owned
//! `String`/`format!` and are not part of any context-allocation surface. The
//! one genuinely palloc'd output — the `TimeLineHistoryEntry` list returned by
//! `readTimeLineHistory` — is built in the caller's `Mcx`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use ::utils_error::errno::current_errno;
use ::utils_error::ereport;
use ::types_error::{ErrorLocation, PgResult, ERROR, FATAL};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::{TimeLineID, XLogRecPtr, InvalidXLogRecPtr};
use ::wal::TimeLineHistoryEntry;
use ::wal::xlog_consts::XLOGDIR;
use ::types_pgstat::wait_event::{
    WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC, WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE,
    WAIT_EVENT_TIMELINE_HISTORY_READ, WAIT_EVENT_TIMELINE_HISTORY_SYNC,
    WAIT_EVENT_TIMELINE_HISTORY_WRITE,
};

use xlogarchive_seams as xlogarchive;
use fd_seams as fd;
use file_seams as file;
use waitevent_seams as waitevent;

/// `errno`'s thread-local location (set to reproduce `%m` after a syscall the
/// way timeline.c relies on `errno` being live when it calls `ereport`).
#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}
#[cfg(not(target_os = "macos"))]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

fn set_errno(value: i32) {
    unsafe {
        *errno_location() = value;
    }
}

/// `XLogRecPtrIsInvalid(r)` (`access/xlogdefs.h`).
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}

/// The canonical history-file *name* for `tli`
/// (`TLHistoryFileName`: `"%08X.history"`, `access/xlog_internal.h`).
pub fn TLHistoryFileName(tli: TimeLineID) -> String {
    format!("{tli:08X}.history")
}

/// The canonical history-file *path* for `tli`
/// (`TLHistoryFilePath`: `XLOGDIR "/%08X.history"`, `access/xlog_internal.h`).
pub fn TLHistoryFilePath(tli: TimeLineID) -> String {
    format!("{XLOGDIR}/{tli:08X}.history")
}

/// Source location stamped onto raised errors (C's `__FILE__`/line macro site).
/// The message text, SQLSTATE, and level carry behavioral parity; the line is
/// not load-bearing.
fn here() -> ErrorLocation {
    ErrorLocation::new("timeline.c", 0, "")
}

/// Copies all timeline history files with id's between `begin` and `end` from
/// archive to `pg_wal` (`restoreTimeLineHistoryFiles`, timeline.c:49-65).
pub fn restoreTimeLineHistoryFiles(
    begin: TimeLineID,
    end: TimeLineID,
    archive_recovery_requested: bool,
    mcx: Mcx<'_>,
) -> PgResult<()> {
    let mut tli = begin;
    while tli < end {
        if tli == 1 {
            tli += 1;
            continue;
        }

        let histfname = TLHistoryFileName(tli);
        // RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0, false).
        let _ = archive_recovery_requested; // restore is the archive path
        if let Some(path) = xlogarchive::restore_archived_history_file::call(mcx, &histfname)? {
            xlogarchive::keep_file_restored_from_archive::call(path.as_str(), &histfname)?;
        }

        tli += 1;
    }
    Ok(())
}

/// Try to read a timeline's history file (`readTimeLineHistory`,
/// timeline.c:75-216).
///
/// On success returns the list of component TLIs (the given TLI followed by its
/// ancestor TLIs), newest first (C's `lcons`-built `List`). If the history file
/// is missing, assumes the timeline has no parents and returns a list of just
/// `targetTLI`. Allocated in `mcx`.
pub fn readTimeLineHistory<'mcx>(
    mcx: Mcx<'mcx>,
    targetTLI: TimeLineID,
    archive_recovery_requested: bool,
) -> PgResult<PgVec<'mcx, TimeLineHistoryEntry>> {
    let mut lasttli: TimeLineID = 0;
    let mut from_archive = false;

    /* Timeline 1 does not have a history file, so no need to check */
    if targetTLI == 1 {
        let mut result = vec_with_capacity_in(mcx, 1)?;
        result.push(TimeLineHistoryEntry {
            tli: targetTLI,
            begin: InvalidXLogRecPtr,
            end: InvalidXLogRecPtr,
        });
        return Ok(result);
    }

    let path;
    let mut histfname = String::new();
    if archive_recovery_requested {
        histfname = TLHistoryFileName(targetTLI);
        match xlogarchive::restore_archived_history_file::call(mcx, &histfname)? {
            Some(restored) => {
                from_archive = true;
                path = restored.as_str().to_string();
            }
            None => {
                from_archive = false;
                /*
                 * RestoreArchivedFile() leaves `path` untouched on failure; the
                 * subsequent open then fails with ENOENT, taking the "not there,
                 * so assume no parents" branch below.
                 */
                path = String::new();
            }
        }
    } else {
        path = TLHistoryFilePath(targetTLI);
    }

    let contents = match fd::read_file_or_absent::call(mcx, &path)? {
        Some(bytes) => bytes,
        None => {
            /* Not there, so assume no parents */
            let mut result = vec_with_capacity_in(mcx, 1)?;
            result.push(TimeLineHistoryEntry {
                tli: targetTLI,
                begin: InvalidXLogRecPtr,
                end: InvalidXLogRecPtr,
            });
            return Ok(result);
        }
    };

    let mut result: PgVec<'mcx, TimeLineHistoryEntry> = PgVec::new_in(mcx);

    /*
     * Parse the file...
     */
    let mut prevend: XLogRecPtr = InvalidXLogRecPtr;
    for fline in history_file_lines(&contents) {
        /* skip leading whitespace and check for # comment */
        let mut first_nonspace: Option<char> = None;
        for ch in fline.chars() {
            if !is_c_space(ch) {
                first_nonspace = Some(ch);
                break;
            }
        }
        match first_nonspace {
            None | Some('#') => continue,
            _ => {}
        }

        let (nfields, tli, switchpoint_hi, switchpoint_lo) = sscanf_history_line(&fline);

        if nfields < 1 {
            /* expect a numeric timeline ID as first field of line */
            return Err(ereport(FATAL)
                .errmsg(format!("syntax error in history file: {fline}"))
                .errhint("Expected a numeric timeline ID.")
                .into_error()
                .with_error_location(here()));
        }
        if nfields != 3 {
            return Err(ereport(FATAL)
                .errmsg(format!("syntax error in history file: {fline}"))
                .errhint("Expected a write-ahead log switchpoint location.")
                .into_error()
                .with_error_location(here()));
        }

        if !result.is_empty() && tli <= lasttli {
            return Err(ereport(FATAL)
                .errmsg(format!("invalid data in history file: {fline}"))
                .errhint("Timeline IDs must be in increasing sequence.")
                .into_error()
                .with_error_location(here()));
        }

        lasttli = tli;

        let entry = TimeLineHistoryEntry {
            tli,
            begin: prevend,
            end: ((switchpoint_hi as u64) << 32) | (switchpoint_lo as u64),
        };
        prevend = entry.end;

        /* Build list with newest item first (lcons) */
        result.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<TimeLineHistoryEntry>()))?;
        result.insert(0, entry);

        /* we ignore the remainder of each line */
    }

    if !result.is_empty() && targetTLI <= lasttli {
        return Err(ereport(FATAL)
            .errmsg(format!("invalid data in history file \"{path}\""))
            .errhint("Timeline IDs must be less than child timeline's ID.")
            .into_error()
            .with_error_location(here()));
    }

    /*
     * Create one more entry for the "tip" of the timeline, which has no entry
     * in the history file.
     */
    let entry = TimeLineHistoryEntry {
        tli: targetTLI,
        begin: prevend,
        end: InvalidXLogRecPtr,
    };

    result.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<TimeLineHistoryEntry>()))?;
    result.insert(0, entry);

    /*
     * If the history file was fetched from archive, save it in pg_wal for future
     * reference.
     */
    if from_archive {
        xlogarchive::keep_file_restored_from_archive::call(&path, &histfname)?;
    }

    Ok(result)
}

/// Probe whether a timeline history file exists for `probeTLI`
/// (`existsTimeLineHistory`, timeline.c:221-254).
pub fn existsTimeLineHistory(
    probeTLI: TimeLineID,
    archive_recovery_requested: bool,
    mcx: Mcx<'_>,
) -> PgResult<bool> {
    /* Timeline 1 does not have a history file, so no need to check */
    if probeTLI == 1 {
        return Ok(false);
    }

    let path;
    if archive_recovery_requested {
        let histfname = TLHistoryFileName(probeTLI);
        match xlogarchive::restore_archived_history_file::call(mcx, &histfname)? {
            Some(restored) => path = restored.as_str().to_string(),
            None => path = String::new(),
        }
    } else {
        path = TLHistoryFilePath(probeTLI);
    }

    fd::file_exists::call(&path)
}

/// Find the newest existing timeline, assuming `startTLI` exists
/// (`findNewestTimeLine`, timeline.c:263-289).
pub fn findNewestTimeLine(
    startTLI: TimeLineID,
    archive_recovery_requested: bool,
    mcx: Mcx<'_>,
) -> PgResult<TimeLineID> {
    let mut newestTLI = startTLI;

    let mut probeTLI = startTLI.wrapping_add(1);
    loop {
        if existsTimeLineHistory(probeTLI, archive_recovery_requested, mcx)? {
            newestTLI = probeTLI; /* probeTLI exists */
        } else {
            /* doesn't exist, assume we're done */
            break;
        }
        probeTLI = probeTLI.wrapping_add(1);
    }

    Ok(newestTLI)
}

/// `BLCKSZ` (`pg_config.h`) — the size of the parent-copy read buffer.
const BLCKSZ: usize = 8192;

/// `XLOGDIR "/xlogtemp.%d"` with the current pid (timeline.c's `snprintf`).
fn xlog_temp_path() -> String {
    let pid = unsafe { libc::getpid() } as i32;
    format!("{XLOGDIR}/xlogtemp.{pid}")
}

/// Create a new timeline history file (`writeTimeLineHistory`,
/// timeline.c:303-453).
pub fn writeTimeLineHistory(
    newTLI: TimeLineID,
    parentTLI: TimeLineID,
    switchpoint: XLogRecPtr,
    reason: &str,
    archive_recovery_requested: bool,
    xlog_archiving_active: bool,
    mcx: Mcx<'_>,
) -> PgResult<()> {
    debug_assert!(newTLI > parentTLI); /* else bad selection of newTLI */

    /*
     * Write into a temp file name.
     */
    let tmppath = xlog_temp_path();

    unsafe {
        let c = std::ffi::CString::new(tmppath.as_bytes()).unwrap();
        libc::unlink(c.as_ptr());
    }

    /* do not use get_sync_bit() here --- want to fsync only at end of fill */
    let fd_ = file::open_transient_file::call(&tmppath, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
    if fd_ < 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }

    /*
     * If a history file exists for the parent, copy it verbatim.
     */
    let path;
    if archive_recovery_requested {
        let histfname = TLHistoryFileName(parentTLI);
        /* RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0, false). */
        match xlogarchive::restore_archived_history_file::call(mcx, &histfname)? {
            Some(restored) => path = restored.as_str().to_string(),
            None => path = String::new(),
        }
    } else {
        path = TLHistoryFilePath(parentTLI);
    }

    // C's `OpenTransientFile` returns -1 with errno set on failure; this
    // caller deliberately tolerates ENOENT ("parent has no parents"). The
    // `fd_seams` i32-contract seam mirrors that exactly (negative `-errno` on
    // failure), so use it here rather than the `file_seams` PgResult seam,
    // which would ereport on ENOENT before this branch can inspect errno.
    let srcfd = fd::open_transient_file::call(&path, libc::O_RDONLY);
    if srcfd < 0 {
        set_errno(-srcfd);
        if current_errno() != libc::ENOENT {
            return Err(ereport(ERROR)
                .with_saved_errno(current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m"))
                .into_error()
                .with_error_location(here()));
        }
        /* Not there, so assume parent has no parents */
    } else {
        let mut buffer = [0u8; BLCKSZ];
        loop {
            set_errno(0);
            waitevent::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_READ);
            let nbytes = unsafe {
                libc::read(srcfd, buffer.as_mut_ptr() as *mut libc::c_void, buffer.len())
            };
            waitevent::pgstat_report_wait_end::call();
            if nbytes < 0 || current_errno() != 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(current_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not read file \"{path}\": %m"))
                    .into_error()
                    .with_error_location(here()));
            }
            if nbytes == 0 {
                break;
            }
            let nbytes = nbytes as usize;
            set_errno(0);
            waitevent::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_WRITE);
            let written = unsafe {
                libc::write(fd_, buffer.as_ptr() as *const libc::c_void, nbytes)
            };
            if written != nbytes as isize {
                let save_errno = current_errno();

                /*
                 * If we fail to make the file, delete it to release disk space
                 */
                unsafe {
                    let c = std::ffi::CString::new(tmppath.as_bytes()).unwrap();
                    libc::unlink(c.as_ptr());
                }

                /* if write didn't set errno, assume problem is no disk space */
                let en = if save_errno != 0 { save_errno } else { libc::ENOSPC };

                return Err(ereport(ERROR)
                    .with_saved_errno(en)
                    .errcode_for_file_access()
                    .errmsg(format!("could not write to file \"{tmppath}\": %m"))
                    .into_error()
                    .with_error_location(here()));
            }
            waitevent::pgstat_report_wait_end::call();
        }

        if file::close_transient_file::call(srcfd) != 0 {
            return Err(ereport(ERROR)
                .with_saved_errno(current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{path}\": %m"))
                .into_error()
                .with_error_location(here()));
        }
    }

    /*
     * Append one line with the details of this timeline split.
     *
     * If we did have a parent file, insert an extra newline just in case the
     * parent file failed to end with one.
     */
    let line = format!(
        "{}{}\t{:X}/{:X}\t{}\n",
        if srcfd < 0 { "" } else { "\n" },
        parentTLI,
        (switchpoint >> 32) as u32,
        switchpoint as u32,
        reason,
    );
    let line_bytes = line.as_bytes();

    set_errno(0);
    waitevent::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_WRITE);
    let written = unsafe {
        libc::write(fd_, line_bytes.as_ptr() as *const libc::c_void, line_bytes.len())
    };
    if written != line_bytes.len() as isize {
        let save_errno = current_errno();
        unsafe {
            let c = std::ffi::CString::new(tmppath.as_bytes()).unwrap();
            libc::unlink(c.as_ptr());
        }
        let en = if save_errno != 0 { save_errno } else { libc::ENOSPC };
        return Err(ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }
    waitevent::pgstat_report_wait_end::call();

    waitevent::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_SYNC);
    if file::pg_fsync::call(fd_) != 0 {
        return Err(ereport(file::data_sync_elevel::call(ERROR))
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }
    waitevent::pgstat_report_wait_end::call();

    if file::close_transient_file::call(fd_) != 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }

    /*
     * Now move the completed history file into place with its final name.
     */
    let path = TLHistoryFilePath(newTLI);
    debug_assert!({
        let c = std::ffi::CString::new(path.as_bytes()).unwrap();
        unsafe { libc::access(c.as_ptr(), libc::F_OK) != 0 && current_errno() == libc::ENOENT }
    });
    file::durable_rename::call(&tmppath, &path, ERROR)?;

    /* The history file can be archived immediately. */
    if xlog_archiving_active {
        let histfname = TLHistoryFileName(newTLI);
        xlogarchive::xlog_archive_notify::call(histfname)?;
    }

    Ok(())
}

/// Writes a history file for given timeline and contents
/// (`writeTimeLineHistoryFile`, timeline.c:462-520).
pub fn writeTimeLineHistoryFile(tli: TimeLineID, content: &[u8]) -> PgResult<()> {
    /*
     * Write into a temp file name.
     */
    let tmppath = xlog_temp_path();

    unsafe {
        let c = std::ffi::CString::new(tmppath.as_bytes()).unwrap();
        libc::unlink(c.as_ptr());
    }

    /* do not use get_sync_bit() here --- want to fsync only at end of fill */
    let fd_ = file::open_transient_file::call(&tmppath, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
    if fd_ < 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }

    set_errno(0);
    waitevent::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE);
    let written = unsafe {
        libc::write(fd_, content.as_ptr() as *const libc::c_void, content.len())
    };
    if written != content.len() as isize {
        let save_errno = current_errno();
        unsafe {
            let c = std::ffi::CString::new(tmppath.as_bytes()).unwrap();
            libc::unlink(c.as_ptr());
        }
        let en = if save_errno != 0 { save_errno } else { libc::ENOSPC };
        return Err(ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }
    waitevent::pgstat_report_wait_end::call();

    waitevent::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC);
    if file::pg_fsync::call(fd_) != 0 {
        return Err(ereport(file::data_sync_elevel::call(ERROR))
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }
    waitevent::pgstat_report_wait_end::call();

    if file::close_transient_file::call(fd_) != 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
            .into_error()
            .with_error_location(here()));
    }

    /*
     * Now move the completed history file into place with its final name,
     * replacing any existing file with the same name.
     */
    let path = TLHistoryFilePath(tli);
    file::durable_rename::call(&tmppath, &path, ERROR)
}

/// Returns true if `expectedTLEs` contains a timeline with id `tli`
/// (`tliInHistory`, timeline.c:525-537).
pub fn tliInHistory(tli: TimeLineID, expectedTLEs: &[TimeLineHistoryEntry]) -> bool {
    for cell in expectedTLEs {
        if cell.tli == tli {
            return true;
        }
    }

    false
}

/// Returns the ID of the timeline in use at a particular point in time, in the
/// given timeline history (`tliOfPointInHistory`, timeline.c:543-563).
pub fn tliOfPointInHistory(
    ptr: XLogRecPtr,
    history: &[TimeLineHistoryEntry],
) -> PgResult<TimeLineID> {
    for tle in history {
        if (XLogRecPtrIsInvalid(tle.begin) || tle.begin <= ptr)
            && (XLogRecPtrIsInvalid(tle.end) || ptr < tle.end)
        {
            /* found it */
            return Ok(tle.tli);
        }
    }

    /* shouldn't happen. */
    Err(ereport(ERROR)
        .errmsg_internal("timeline history was not contiguous")
        .into_error()
        .with_error_location(here()))
}

/// Returns the point in history where we branched off the given timeline, and
/// the timeline we branched to (the `.1` of the tuple). Returns
/// `InvalidXLogRecPtr`/`next = 0` if the timeline is current; errors if the
/// timeline is not part of this server's history (`tliSwitchPoint`,
/// timeline.c:571-592). The C out-parameter `*nextTLI` is returned as the
/// tuple's second element.
pub fn tliSwitchPoint(
    tli: TimeLineID,
    history: &[TimeLineHistoryEntry],
) -> PgResult<(XLogRecPtr, TimeLineID)> {
    let mut nextTLI: TimeLineID = 0;
    for tle in history {
        if tle.tli == tli {
            return Ok((tle.end, nextTLI));
        }
        nextTLI = tle.tli;
    }

    Err(ereport(ERROR)
        .errmsg(format!(
            "requested timeline {tli} is not in this server's history"
        ))
        .into_error()
        .with_error_location(here()))
}

/// Install this crate's seam implementations
/// (`backend-access-transam-timeline-seams`).
pub fn init_seams() {
    use timeline_seams as seams;
    // `read_timeline_history(mcx, target_tli)` is the live cyclic-consumer
    // surface (xlogutils `XLogReadDetermineTimeline`, walsummarizer): these
    // run during normal operation / streaming, where `ArchiveRecoveryRequested`
    // is its default `false` (it is only set true during archive-recovery
    // startup in xlogrecovery.c). So the 2-arg seam is the pg_wal-path reader;
    // it threads `archive_recovery_requested = false` into the full impl.
    seams::read_timeline_history::set(|mcx, target_tli| {
        readTimeLineHistory(mcx, target_tli, false)
    });
    seams::exists_timeline_history::set(|mcx, probe_tli, arr| {
        existsTimeLineHistory(probe_tli, arr, mcx)
    });
    seams::find_newest_timeline::set(|mcx, start_tli, arr| {
        findNewestTimeLine(start_tli, arr, mcx)
    });
    seams::write_timeline_history::set(
        |mcx, new_tli, parent_tli, sp, reason, arr, xaa| {
            writeTimeLineHistory(new_tli, parent_tli, sp, reason, arr, xaa, mcx)
        },
    );
    seams::write_timeline_history_file::set(writeTimeLineHistoryFile);
    seams::restore_timeline_history_files::set(|mcx, begin, end, arr| {
        restoreTimeLineHistoryFiles(begin, end, arr, mcx)
    });
    seams::tli_in_history::set(tliInHistory);
    seams::tli_of_point_in_history::set(tliOfPointInHistory);
    seams::tli_switch_point::set(tliSwitchPoint);
    seams::tl_history_file_name::set(TLHistoryFileName);
    seams::tl_history_file_path::set(TLHistoryFilePath);
}

/* ---------------------------------------------------------------------------
 * Pure parsing helpers
 *
 * These reproduce, byte-for-byte, the behaviour of the C parsing path in
 * readTimeLineHistory: the fgets() line splitting, the isspace()/'#' comment
 * test, and the `sscanf(fline, "%u\t%X/%X", ...)` field extraction.
 * ---------------------------------------------------------------------------
 */

/// Split the raw history-file bytes into lines the way C's `fgets()` loop sees
/// them: each line retains its trailing `'\n'` (the final line may lack one).
/// Bytes are interpreted leniently as Latin-1 so the message text and
/// whitespace tests reproduce the C `char`-based logic; ASCII content (the only
/// legal history-file content) round-trips unchanged.
fn history_file_lines(contents: &[u8]) -> Vec<String> {
    // C reads each line with `fgets(fline, MAXPGPATH, fd)` into a 1024-byte
    // buffer, so a physical line is split into chunks of at most MAXPGPATH-1
    // (1023) bytes: fgets returns when it sees a newline (kept) OR the buffer
    // fills, whichever comes first.
    const FGETS_MAX: usize = 1024 - 1;
    let mut lines = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < contents.len() {
        let at_newline = contents[i] == b'\n';
        let buffer_full = i - start + 1 == FGETS_MAX;
        if at_newline || buffer_full {
            lines.push(chunk_to_cstr(&contents[start..=i]));
            start = i + 1;
        }
        i += 1;
    }
    if start < contents.len() {
        lines.push(chunk_to_cstr(&contents[start..]));
    }
    lines
}

/// Convert one `fgets` chunk to the string C's `char`-based parsing sees: bytes
/// interpreted leniently as Latin-1, truncated at the first embedded NUL (every
/// C string op on `fline` stops at the first `'\0'`).
fn chunk_to_cstr(bytes: &[u8]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    bytes_to_latin1(&bytes[..end])
}

fn bytes_to_latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect()
}

/// `isspace((unsigned char) c)` for the "C" locale (space, form-feed, newline,
/// carriage return, horizontal & vertical tab).
fn is_c_space(c: char) -> bool {
    matches!(c, ' ' | '\u{0c}' | '\n' | '\r' | '\t' | '\u{0b}')
}

/// `sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo)`.
///
/// Returns `(nfields, tli, switchpoint_hi, switchpoint_lo)` where `nfields` is
/// scanf's conversion count (0..=3). Whitespace in the format (the tab) matches
/// any run of whitespace (possibly empty); the non-whitespace `'/'` literal must
/// match exactly or the scan stops.
fn sscanf_history_line(fline: &str) -> (i32, TimeLineID, u32, u32) {
    let bytes = fline.as_bytes();
    let mut pos = 0usize;

    /* %u -- skip leading whitespace, then decimal digits */
    let Some((tli, next)) = scan_decimal_u32(bytes, pos) else {
        return (0, 0, 0, 0);
    };
    pos = next;

    /* literal '\t' in the format string: matches any whitespace run */
    pos = skip_whitespace(bytes, pos);

    /* %X -- skip leading whitespace, then hex digits */
    let Some((hi, next)) = scan_hex_u32(bytes, pos) else {
        return (1, tli, 0, 0);
    };
    pos = next;

    /* literal '/' -- must match exactly */
    if pos >= bytes.len() || bytes[pos] != b'/' {
        return (2, tli, hi, 0);
    }
    pos += 1;

    /* %X -- skip leading whitespace, then hex digits */
    let Some((lo, _next)) = scan_hex_u32(bytes, pos) else {
        return (2, tli, hi, 0);
    };

    (3, tli, hi, lo)
}

/// Skip a run of `isspace` characters (in C locale) starting at `pos`.
fn skip_whitespace(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && is_c_space(bytes[pos] as char) {
        pos += 1;
    }
    pos
}

/// `%u`: scanf skips leading whitespace, then reads one or more decimal digits,
/// accumulating into a 32-bit `unsigned int` (wrapping on overflow, matching
/// glibc). Returns `None` if no digit is found.
fn scan_decimal_u32(bytes: &[u8], pos: usize) -> Option<(u32, usize)> {
    let mut i = skip_whitespace(bytes, pos);
    /* glibc's %u accepts an optional leading sign; '-' negates with wraparound */
    let mut negate = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        negate = bytes[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut value: u32 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        value = value.wrapping_mul(10).wrapping_add((bytes[i] - b'0') as u32);
        i += 1;
    }
    if i == start {
        None
    } else {
        Some((if negate { value.wrapping_neg() } else { value }, i))
    }
}

/// `%X`: scanf skips leading whitespace, then reads one or more hexadecimal
/// digits (an optional `0x`/`0X` prefix is consumed), accumulating into a
/// 32-bit `unsigned int` (wrapping on overflow). Returns `None` if no digit is
/// found.
fn scan_hex_u32(bytes: &[u8], pos: usize) -> Option<(u32, usize)> {
    let mut i = skip_whitespace(bytes, pos);

    /* glibc's %X accepts an optional leading sign (before any 0x prefix) */
    let mut negate = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        negate = bytes[i] == b'-';
        i += 1;
    }

    /* optional 0x / 0X prefix */
    if i + 1 < bytes.len()
        && bytes[i] == b'0'
        && (bytes[i + 1] == b'x' || bytes[i + 1] == b'X')
        && i + 2 < bytes.len()
        && bytes[i + 2].is_ascii_hexdigit()
    {
        i += 2;
    }

    let start = i;
    let mut value: u32 = 0;
    while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
        let digit = (bytes[i] as char).to_digit(16).unwrap();
        value = value.wrapping_mul(16).wrapping_add(digit);
        i += 1;
    }
    if i == start {
        None
    } else {
        Some((if negate { value.wrapping_neg() } else { value }, i))
    }
}

#[cfg(test)]
mod tests;
